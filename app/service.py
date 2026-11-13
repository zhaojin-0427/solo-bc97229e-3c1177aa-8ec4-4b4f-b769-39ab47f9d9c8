"""入湖编排：暂存 -> 解析校验 -> 写 Parquet -> 短事务登记目录。

不变量：
1. 解析与写文件期间不持有目录库写事务；
2. 目录只引用已完整落盘的文件（先 rename 后登记；登记失败删文件）；
3. 坏整数 / 写盘失败 / 登记失败 => 整批不发布，不留残件引用；
4. 发布按「提交时生效的鸟种字典」整批做标准化校验，冲突即整批不发布；
   若登记前生效字典发生切换，则按新字典重新校验后重试（不漏检字典切换）。
"""
import datetime
import os
import re
import shutil
import time
import uuid

from . import catalog, csvparse, standardize, store

_TOKEN_RE = re.compile(r"[0-9a-f]{32}")
STAGING_MAX_AGE_SEC = 3600
ORPHAN_MAX_AGE_SEC = 600
REPL_STALE_SEC = 120


def init_storage(cfg):
    os.makedirs(cfg.data_dir, exist_ok=True)
    os.makedirs(cfg.parquet_dir, exist_ok=True)
    os.makedirs(cfg.staging_dir, exist_ok=True)
    os.makedirs(cfg.dict_dir, exist_ok=True)
    os.makedirs(cfg.lock_dir, exist_ok=True)
    catalog.init_db(cfg.db_path)
    _ensure_initial_dictionary(cfg)
    recovered = _recover_stale_replacements(cfg)
    cleanup_staging(cfg)
    cleanup_orphan_files(cfg)
    return recovered


def _ensure_initial_dictionary(cfg):
    """初始沿用原名：v1 为空字典（所有原名映射到自身）。"""
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            row = catalog.get_dictionary(conn, 1)
            if row is None:
                now = datetime.datetime.now(datetime.timezone.utc) \
                    .isoformat(timespec="seconds")
                vid = catalog.insert_dictionary(conn, "dict_v1.json", 0, now)
                assert vid == 1
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('active_dict_version', '1')")
    finally:
        conn.close()
    v1_path = os.path.join(cfg.dict_dir, "dict_v1.json")
    if not os.path.isfile(v1_path):
        standardize.write_dict_file(cfg.dict_dir, 1, {})


def _recover_stale_replacements(cfg):
    """重启恢复：心跳过期的未完成替换标失败；生效字典始终保留不动。"""
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            now = datetime.datetime.now(datetime.timezone.utc)
            ids = catalog.fail_stale_replacements(conn, now, REPL_STALE_SEC)
    finally:
        conn.close()
    return ids


# ---------- 暂存（声明列类型的两步表单之间） ----------

def stage_upload(cfg, stream):
    token = uuid.uuid4().hex
    path = os.path.join(cfg.staging_dir, token + ".csv")
    with open(path, "wb") as out:
        shutil.copyfileobj(stream, out, 256 * 1024)
    return token


def staged_path(cfg, token):
    if not _TOKEN_RE.fullmatch(token or ""):
        raise ValueError("无效的暂存令牌")
    path = os.path.join(cfg.staging_dir, token + ".csv")
    if not os.path.isfile(path):
        raise ValueError("暂存文件不存在或已过期，请重新上传")
    return path


def staged_header(cfg, token):
    return csvparse.read_header(staged_path(cfg, token))


def discard_staged(cfg, token):
    if _TOKEN_RE.fullmatch(token or ""):
        try:
            os.unlink(os.path.join(cfg.staging_dir, token + ".csv"))
        except OSError:
            pass


# ---------- 发布 ----------

def publish(cfg, station, source_filename, csv_path, col_types):
    """校验并发布一个批次。任何失败都整批不发布。"""
    station = (station or "").strip()
    if not station:
        raise ValueError("站点不能为空")
    if len(station) > 200:
        raise ValueError("站点名过长")
    header = csvparse.read_header(csv_path)
    species_names = header[1:]
    if len(species_names) != len(col_types):
        raise ValueError("列类型数量与 CSV 鸟种列数不一致")
    for t in col_types:
        if t not in catalog.COLUMN_TYPES:
            raise ValueError(f"非法列类型: {t!r}")
    if store.DATE_COLUMN in species_names:
        raise csvparse.CsvError(f"鸟种列不能使用保留列名 {store.DATE_COLUMN}")

    # 1) 解析与校验（不持有目录库写事务）；坏整数在此整批拒绝
    dates, values = csvparse.parse_rows(csv_path, col_types)
    columns = list(zip(species_names, col_types, values))
    table = store.build_batch_table(dates, columns)
    rows = [
        {name: col[i] for name, _typ, col in columns}
        for i in range(table.num_rows)
    ]

    # 2) 按提交时生效字典做整批标准化校验（不持有目录库写事务）。
    #    登记事务内若发现字典已切换，则按新字典重新校验后重试，杜绝漏检。
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.read_txn(conn):
            active_version = catalog.get_active_dict_version(conn)
            dict_row = catalog.get_dictionary(conn, active_version)
        mapping = standardize.load_mapping(
            os.path.join(cfg.dict_dir, dict_row["dict_file"]))
    finally:
        conn.close()
    standardize.validate_publish(species_names, rows, mapping)

    # 3) 写独立 Parquet 文件（不持有目录库写事务）；写盘失败整批不发布
    batch_id = uuid.uuid4().hex
    final_name = batch_id + ".parquet"
    final_path = os.path.join(cfg.parquet_dir, final_name)
    store.write_parquet_atomic(table, final_path)

    # 4) 短事务：合并逻辑结构 + 登记批次（并发发布者在此串行）
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            current_version = catalog.get_active_dict_version(conn)
            if current_version != active_version:
                # 校验后字典刚好切换：按新生效字典重检，冲突则整批不发布
                new_row = catalog.get_dictionary(conn, current_version)
                new_mapping = standardize.load_mapping(
                    os.path.join(cfg.dict_dir, new_row["dict_file"]))
                standardize.validate_publish(species_names, rows, new_mapping)
                active_version = current_version
            changes = catalog.merge_schema(conn, [(n, t) for n, t, _ in columns])
            catalog.insert_batch(conn, {
                "id": batch_id,
                "station": station,
                "source_filename": source_filename or "",
                "parquet_file": final_name,
                "n_rows": table.num_rows,
                "created_at": datetime.datetime.now(datetime.timezone.utc)
                              .isoformat(timespec="seconds"),
                "dict_version": active_version,
            })
    except BaseException:
        try:
            os.unlink(final_path)  # 目录未登记 => 删除未发布文件，目录永不引用残件
        except OSError:
            pass
        raise
    finally:
        conn.close()
    return {"batch_id": batch_id, "station": station,
            "n_rows": table.num_rows, "changes": changes,
            "dict_version": active_version}


# ---------- 启动清理 ----------

def cleanup_staging(cfg, max_age_sec=STAGING_MAX_AGE_SEC):
    now = time.time()
    for name in os.listdir(cfg.staging_dir):
        path = os.path.join(cfg.staging_dir, name)
        try:
            if now - os.path.getmtime(path) > max_age_sec:
                os.unlink(path)
        except OSError:
            pass


def cleanup_orphan_files(cfg, max_age_sec=ORPHAN_MAX_AGE_SEC):
    """清理崩溃遗留：临时文件一律删；未被目录引用的 Parquet 超过宽限期才删
    （宽限期避免误删其他进程刚 rename、尚未登记的批次文件）。"""
    conn = catalog.connect(cfg.db_path)
    try:
        referenced = {r["parquet_file"] for r in conn.execute("SELECT parquet_file FROM batches")}
    finally:
        conn.close()
    now = time.time()
    for name in os.listdir(cfg.parquet_dir):
        path = os.path.join(cfg.parquet_dir, name)
        try:
            if name.startswith(".") and name.endswith(".tmp"):
                if now - os.path.getmtime(path) > max_age_sec:
                    os.unlink(path)
            elif name.endswith(".parquet") and name not in referenced:
                if now - os.path.getmtime(path) > max_age_sec:
                    os.unlink(path)
        except OSError:
            pass
