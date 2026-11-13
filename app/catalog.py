"""目录库（SQLite, WAL 模式）：逻辑结构 + 批次清单 + 鸟种字典版本 + 替换任务。

事务纪律：
- 写事务（write_txn）只在「Parquet 文件已完整落盘」之后短暂持有，
  用于合并逻辑结构 + 登记批次，解析/写文件期间绝不持有库写事务。
- 替换扫描本身只读 Parquet、不持有库写事务；进度/冲突由多个短写事务落库。
- 读事务（read_txn）给调用方一个一致快照：同一次取数看到同一状态的
  逻辑结构、文件清单与生效字典版本。
"""
import datetime
import sqlite3
from contextlib import contextmanager

COLUMN_TYPES = ("uint64", "text")

# 替换任务状态
REPL_SCANNING = "scanning"   # 后台核查中（旧字典仍然生效）
REPL_SUCCESS = "success"     # 已切换：新字典生效
REPL_FAILED = "failed"       # 冲突 / 异常 / 重启遗留，未切换
REPL_STATES = (REPL_SCANNING, REPL_SUCCESS, REPL_FAILED)

DDL = """
CREATE TABLE IF NOT EXISTS columns (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,   -- 稳定列序：先出现的列排前
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL CHECK (type IN ('uint64','text'))
);
CREATE TABLE IF NOT EXISTS batches (
  id              TEXT PRIMARY KEY,
  station         TEXT NOT NULL,
  source_filename TEXT NOT NULL,
  parquet_file    TEXT NOT NULL UNIQUE,
  n_rows          INTEGER NOT NULL,
  created_at      TEXT NOT NULL,
  dict_version    INTEGER NOT NULL DEFAULT 1   -- 发布时生效的字典版本
);
CREATE INDEX IF NOT EXISTS idx_batches_station ON batches(station);
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- 鸟种别名字典：每版一个不可变 JSON 文件（原名 -> 标准名，单层不递归）
CREATE TABLE IF NOT EXISTS dictionaries (
  version     INTEGER PRIMARY KEY AUTOINCREMENT,
  dict_file   TEXT NOT NULL UNIQUE,
  n_mappings  INTEGER NOT NULL,
  created_at  TEXT NOT NULL
);
-- 替换任务：同一时刻至多一条 scanning
CREATE TABLE IF NOT EXISTS replacements (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  status          TEXT NOT NULL CHECK (status IN ('scanning','success','failed')),
  old_version     INTEGER NOT NULL,
  new_version     INTEGER NOT NULL,
  progress_batches INTEGER NOT NULL DEFAULT 0,
  total_batches   INTEGER NOT NULL DEFAULT 0,
  n_conflicts     INTEGER NOT NULL DEFAULT 0,
  error           TEXT NOT NULL DEFAULT '',
  heartbeat_at    TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  finished_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_repl_status ON replacements(status);
-- 冲突明细（失败时供页面列出：批次 / 数据行号(从1起) / 标准名 / 原值）
CREATE TABLE IF NOT EXISTS replacement_conflicts (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  replacement_id INTEGER NOT NULL REFERENCES replacements(id),
  batch_id       TEXT NOT NULL,
  station        TEXT NOT NULL,
  source_filename TEXT NOT NULL,
  row_number     INTEGER NOT NULL,
  standard_name  TEXT NOT NULL,
  original_name  TEXT NOT NULL,
  original_value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_conf_repl ON replacement_conflicts(replacement_id);
"""


def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path):
    conn = connect(db_path)
    try:
        conn.executescript(DDL)
        # 旧库升级：batches 缺 dict_version 时补上（历史批次视为 v1 生效期发布）
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(batches)")}
        if "dict_version" not in cols:
            conn.execute("ALTER TABLE batches ADD COLUMN dict_version INTEGER NOT NULL DEFAULT 1")
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '0')")
    finally:
        conn.close()


@contextmanager
def write_txn(conn):
    """短写事务：BEGIN IMMEDIATE 串行化并发发布者。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def read_txn(conn):
    """一致读快照（WAL 下读写不互斥）。"""
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")


def merge_schema(conn, decls):
    """在同一写事务内合并逻辑结构。

    decls: [(name, 'uint64'|'text'), ...]
    规则：新列加入；同名同型不变；同名异型提升为 text（text 吸收，永不退回 uint64）。
    """
    changes = []
    for name, new_type in decls:
        if new_type not in COLUMN_TYPES:
            raise ValueError(f"非法列类型: {new_type!r}")
        row = conn.execute("SELECT type FROM columns WHERE name = ?", (name,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO columns(name, type) VALUES (?, ?)", (name, new_type))
            changes.append({"change": "added", "name": name, "type": new_type})
        elif row["type"] == new_type:
            continue
        elif row["type"] == "text":
            continue  # 已提升为 text，不退回整数
        else:
            conn.execute("UPDATE columns SET type = 'text' WHERE name = ? AND type = 'uint64'", (name,))
            changes.append({"change": "promoted", "name": name, "type": "text"})
    if changes:
        conn.execute(
            "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
            "WHERE key = 'schema_version'"
        )
    return changes


def insert_batch(conn, batch):
    conn.execute(
        "INSERT INTO batches(id, station, source_filename, parquet_file, n_rows,"
        " created_at, dict_version)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (batch["id"], batch["station"], batch["source_filename"],
         batch["parquet_file"], batch["n_rows"], batch["created_at"],
         batch["dict_version"]),
    )


def snapshot(conn, station=None):
    """一个读事务内同时取逻辑结构、批次清单与生效字典版本（同一状态）。"""
    with read_txn(conn):
        cols = [dict(r) for r in conn.execute("SELECT name, type FROM columns ORDER BY id")]
        q = ("SELECT id, station, source_filename, parquet_file, n_rows, created_at,"
             " dict_version FROM batches")
        if station:
            rows = conn.execute(q + " WHERE station = ? ORDER BY rowid", (station,)).fetchall()
        else:
            rows = conn.execute(q + " ORDER BY rowid").fetchall()
        ver = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
        dv = conn.execute("SELECT value FROM meta WHERE key = 'active_dict_version'").fetchone()
    return {
        "schema_version": int(ver["value"]) if ver else 0,
        "active_dict_version": int(dv["value"]) if dv else 1,
        "columns": cols,
        "batches": [dict(r) for r in rows],
    }


# ---------- 鸟种字典 ----------

def insert_dictionary(conn, dict_file, n_mappings, now_iso):
    cur = conn.execute(
        "INSERT INTO dictionaries(dict_file, n_mappings, created_at) VALUES (?, ?, ?)",
        (dict_file, n_mappings, now_iso),
    )
    return cur.lastrowid


def get_dictionary(conn, version):
    return conn.execute(
        "SELECT version, dict_file, n_mappings, created_at FROM dictionaries"
        " WHERE version = ?", (version,)).fetchone()


def list_dictionaries(conn):
    return [dict(r) for r in conn.execute(
        "SELECT version, dict_file, n_mappings, created_at FROM dictionaries ORDER BY version")]


def get_active_dict_version(conn):
    row = conn.execute(
        "SELECT value FROM meta WHERE key = 'active_dict_version'").fetchone()
    return int(row["value"]) if row else 1


def activate_dictionary(conn, version, now_iso):
    """把生效字典版本置为 version（不存在则返回 False）。"""
    exists = conn.execute(
        "SELECT 1 FROM dictionaries WHERE version = ?", (version,)).fetchone()
    if not exists:
        return False
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('active_dict_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (str(version),))
    return True


# ---------- 替换任务 ----------

def get_running_replacement(conn):
    row = conn.execute(
        "SELECT * FROM replacements WHERE status = ? ORDER BY id DESC LIMIT 1",
        (REPL_SCANNING,)).fetchone()
    return dict(row) if row else None


def insert_replacement(conn, old_version, new_version, total_batches, now_iso):
    cur = conn.execute(
        "INSERT INTO replacements(status, old_version, new_version, progress_batches,"
        " total_batches, n_conflicts, error, heartbeat_at, created_at, finished_at)"
        " VALUES (?, ?, ?, 0, ?, 0, '', ?, ?, NULL)",
        (REPL_SCANNING, old_version, new_version, total_batches, now_iso, now_iso))
    return cur.lastrowid


def heartbeat_replacement(conn, repl_id, now_iso):
    conn.execute("UPDATE replacements SET heartbeat_at = ? WHERE id = ?", (now_iso, repl_id))


def update_replacement_progress(conn, repl_id, progress, n_conflicts, now_iso):
    conn.execute(
        "UPDATE replacements SET progress_batches = ?, n_conflicts = ?, heartbeat_at = ?"
        " WHERE id = ?", (progress, n_conflicts, now_iso, repl_id))


def add_replacement_conflict(conn, repl_id, conflict):
    conn.execute(
        "INSERT INTO replacement_conflicts(replacement_id, batch_id, station,"
        " source_filename, row_number, standard_name, original_name, original_value)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (repl_id, conflict["batch_id"], conflict["station"], conflict["source_filename"],
         conflict["row_number"], conflict["standard_name"], conflict["original_name"],
         conflict["original_value"]))


def finish_replacement(conn, repl_id, status, error, now_iso):
    conn.execute(
        "UPDATE replacements SET status = ?, error = ?, finished_at = ?, heartbeat_at = ?"
        " WHERE id = ?", (status, error or "", now_iso, now_iso, repl_id))


def get_replacement(conn, repl_id):
    row = conn.execute("SELECT * FROM replacements WHERE id = ?", (repl_id,)).fetchone()
    return dict(row) if row else None


def latest_replacement(conn):
    row = conn.execute("SELECT * FROM replacements ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def list_replacements(conn, limit=20):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM replacements ORDER BY id DESC LIMIT ?", (limit,))]


def list_conflicts(conn, repl_id, limit=500):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM replacement_conflicts WHERE replacement_id = ?"
        " ORDER BY id LIMIT ?", (repl_id, limit))]


def count_conflicts(conn, repl_id):
    return conn.execute(
        "SELECT COUNT(*) AS n FROM replacement_conflicts WHERE replacement_id = ?",
        (repl_id,)).fetchone()["n"]


def fail_stale_replacements(conn, now_dt, max_age_sec):
    """启动恢复：心跳过期的 scanning 任务标记失败（持有者进程已死 / 重启）。

    心跳仍新鲜说明可能是其他进程正在执行，不得触碰。
    """
    stale = conn.execute(
        "SELECT id, heartbeat_at FROM replacements WHERE status = ?",
        (REPL_SCANNING,)).fetchall()
    ids = []
    for r in stale:
        try:
            hb = datetime.datetime.fromisoformat(r["heartbeat_at"])
        except ValueError:
            hb = None
        if hb is None or (now_dt - hb).total_seconds() > max_age_sec:
            ids.append(r["id"])
    if ids:
        conn.execute(
            "UPDATE replacements SET status = ?, error = ?, finished_at = ?,"
            " heartbeat_at = ? WHERE id IN (%s)" % ",".join("?" * len(ids)),
            [REPL_FAILED, "进程重启或心跳超时，替换未完成，已标记失败",
             now_dt.isoformat(timespec="seconds"), now_dt.isoformat(timespec="seconds")] + ids)
    return ids


def batches_for_scan(conn, after_rowid=0, limit=None):
    """扫描用批次清单（只读短事务），按 rowid 稳定顺序，支持增量追赶。"""
    sql = ("SELECT rowid AS rid, id, station, source_filename, parquet_file, n_rows"
           " FROM batches WHERE rowid > ? ORDER BY rowid")
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(r) for r in conn.execute(sql, (after_rowid,))]


def max_batch_rowid(conn):
    row = conn.execute("SELECT COALESCE(MAX(rowid), 0) AS m FROM batches").fetchone()
    return row["m"]
