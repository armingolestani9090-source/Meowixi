# ==========================================
# DB pool — Local SQLite (Termux) or PostgreSQL
# ==========================================
from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from typing import Any

from config import LOCAL_MODE, SQLITE_PATH, DATABASE_URL, DB_POOL_MIN, DB_POOL_MAX

logger = logging.getLogger("meowbot.db")

_pool = None
_sqlite: sqlite3.Connection | None = None
_lock = asyncio.Lock()


def _to_sqlite_sql(query: str, args: tuple = ()) -> tuple[str, tuple]:
    """Convert asyncpg $1,$2 placeholders to sqlite ? (expand repeats)."""
    q = query
    q = re.sub(r"::\w+", "", q)
    q = re.sub(r"\bILIKE\b", "LIKE", q, flags=re.IGNORECASE)
    q = re.sub(r"\bNOW\s*\(\s*\)", "datetime('now')", q, flags=re.IGNORECASE)
    # PostgreSQL row locks / tips not supported by SQLite
    q = re.sub(r"\bFOR\s+UPDATE\b", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\bFOR\s+SHARE\b", "", q, flags=re.IGNORECASE)
    new_args: list = []

    def repl(m):
        idx = int(m.group(1)) - 1
        if idx < 0 or idx >= len(args):
            raise IndexError(
                f"SQL placeholder ${idx + 1} out of range for {len(args)} args"
            )
        new_args.append(args[idx])
        return "?"

    q = re.sub(r"\$(\d+)", repl, q)
    return q, tuple(new_args)


class _Row(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as e:
            raise AttributeError(item) from e


def _connect_sqlite() -> sqlite3.Connection:
    path = SQLITE_PATH or ":memory:"
    if path == ":memory:":
        conn = sqlite3.connect(
            "file:meowbot_local?mode=memory&cache=shared",
            uri=True,
            check_same_thread=False,
        )
    else:
        conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn



class _SqliteTx:
    """Nested transaction no-op; outer acquire handles commit/rollback."""
    def __init__(self, proxy: "_SqliteConnProxy"):
        self._proxy = proxy

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Real commit/rollback happens in acquire context
        if exc_type is not None:
            self._proxy._failed = True
        return False


class _SqliteConnProxy:
    """asyncpg-like connection surface over a shared sqlite3 connection."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._failed = False

    def transaction(self):
        return _SqliteTx(self)

    async def fetch(self, query: str, *args):
        def _run():
            sql, bind = _to_sqlite_sql(query, args)
            cur = self._conn.execute(sql, bind)
            rows = cur.fetchall()
            desc = cur.description
            if not desc:
                return []
            return [_Row({desc[i][0]: r[i] for i in range(len(desc))}) for r in rows]

        return await asyncio.to_thread(_run)

    async def fetchrow(self, query: str, *args):
        def _run():
            sql, bind = _to_sqlite_sql(query, args)
            cur = self._conn.execute(sql, bind)
            row = cur.fetchone()
            if row is None:
                return None
            desc = cur.description
            return _Row({desc[i][0]: row[i] for i in range(len(desc))})

        return await asyncio.to_thread(_run)

    async def fetchval(self, query: str, *args):
        row = await self.fetchrow(query, *args)
        if row is None:
            return None
        return next(iter(row.values()))

    async def execute(self, query: str, *args):
        def _run():
            sql, bind = _to_sqlite_sql(query, args)
            cur = self._conn.execute(sql, bind)
            # mimic asyncpg status string roughly
            if cur.rowcount is not None and cur.rowcount >= 0:
                return f"UPDATE {cur.rowcount}" if sql.strip().upper().startswith("UPDATE") else f"OK {cur.rowcount}"
            return "OK"

        return await asyncio.to_thread(_run)


class _SqliteAcquire:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._proxy: _SqliteConnProxy | None = None

    async def __aenter__(self) -> _SqliteConnProxy:
        await _lock.acquire()
        def _begin():
            try:
                self._conn.execute("BEGIN")
            except sqlite3.OperationalError:
                # already in transaction
                pass
        await asyncio.to_thread(_begin)
        self._proxy = _SqliteConnProxy(self._conn)
        return self._proxy

    async def __aexit__(self, exc_type, exc, tb):
        proxy = self._proxy
        failed = bool(exc_type) or (proxy is not None and proxy._failed)

        def _end():
            try:
                if failed:
                    self._conn.rollback()
                else:
                    self._conn.commit()
            except Exception:
                try:
                    self._conn.rollback()
                except Exception:
                    pass

        try:
            await asyncio.to_thread(_end)
        finally:
            _lock.release()
        return False


class SqlitePoolAdapter:
    """Expose .acquire() like asyncpg pool for LOCAL SQLite mode."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def acquire(self):
        return _SqliteAcquire(self._conn)



async def init_pool():
    global _pool, _sqlite
    if LOCAL_MODE or not DATABASE_URL:
        if _sqlite is not None:
            return SqlitePoolAdapter(_sqlite)
        _sqlite = await asyncio.to_thread(_connect_sqlite)
        logger.info("✅ Local SQLite ready path=%s (no Railway)", SQLITE_PATH or ":memory:")
        return SqlitePoolAdapter(_sqlite)

    if _pool is not None:
        return _pool
    import asyncpg

    dsn = DATABASE_URL
    if dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=DB_POOL_MIN,
        max_size=DB_POOL_MAX,
        command_timeout=60,
    )
    logger.info("✅ PostgreSQL pool created")
    return _pool


async def get_pool():
    if LOCAL_MODE or not DATABASE_URL:
        if _sqlite is None:
            return await init_pool()
        return SqlitePoolAdapter(_sqlite)
    if _pool is None:
        return await init_pool()
    return _pool


async def close_pool():
    global _pool, _sqlite
    if _sqlite is not None:
        await asyncio.to_thread(_sqlite.close)
        _sqlite = None
        logger.info("SQLite closed")
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL pool closed")


async def fetch(query: str, *args):
    if LOCAL_MODE or not DATABASE_URL:
        async with _lock:
            if _sqlite is None:
                await init_pool()
            conn = _sqlite

            def _run():
                sql, bind = _to_sqlite_sql(query, args)
                cur = conn.execute(sql, bind)
                rows = cur.fetchall()
                desc = cur.description
                return [
                    _Row({desc[i][0]: r[i] for i in range(len(desc))}) for r in rows
                ]

            return await asyncio.to_thread(_run)
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def fetchrow(query: str, *args):
    if LOCAL_MODE or not DATABASE_URL:
        async with _lock:
            if _sqlite is None:
                await init_pool()
            conn = _sqlite

            def _run():
                sql, bind = _to_sqlite_sql(query, args)
                cur = conn.execute(sql, bind)
                row = cur.fetchone()
                if row is None:
                    return None
                return _Row(
                    {cur.description[i][0]: row[i] for i in range(len(cur.description))}
                )

            return await asyncio.to_thread(_run)
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetchval(query: str, *args):
    if LOCAL_MODE or not DATABASE_URL:
        async with _lock:
            if _sqlite is None:
                await init_pool()
            conn = _sqlite

            def _run():
                sql, bind = _to_sqlite_sql(query, args)
                cur = conn.execute(sql, bind)
                row = cur.fetchone()
                return None if row is None else row[0]

            return await asyncio.to_thread(_run)
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(query, *args)


async def execute(query: str, *args):
    if LOCAL_MODE or not DATABASE_URL:
        async with _lock:
            if _sqlite is None:
                await init_pool()
            conn = _sqlite

            def _run():
                sql, bind = _to_sqlite_sql(query, args)
                cur = conn.execute(sql, bind)
                conn.commit()
                rc = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
                q = query.lstrip().split(None, 1)[0].upper() if query.strip() else "OK"
                return f"{q} {rc}"

            return await asyncio.to_thread(_run)
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)


async def executemany(query: str, args_list):
    if LOCAL_MODE or not DATABASE_URL:
        async with _lock:
            if _sqlite is None:
                await init_pool()
            conn = _sqlite

            def _run():
                if not args_list:
                    return
                sql, _ = _to_sqlite_sql(query, tuple(args_list[0]))
                bind_rows = [_to_sqlite_sql(query, tuple(a))[1] for a in args_list]
                conn.executemany(sql, bind_rows)
                conn.commit()

            return await asyncio.to_thread(_run)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(query, args_list)


def affected_rows(status: Any) -> bool:
    if status is None:
        return False
    s = str(status)
    parts = s.split()
    for p in reversed(parts):
        if p.isdigit():
            return int(p) > 0
    return False
