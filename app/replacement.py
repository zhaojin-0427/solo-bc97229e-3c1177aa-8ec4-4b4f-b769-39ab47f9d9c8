"""鸟种字典替换编排：后台核查全部已发布观测 -> 无冲突才原子切换。

并发与一致性纪律：
1. 替换全程持有跨进程排他 flock，多进程共用 DATA_DIR 时同时只允许一次替换；
2. 扫描只读 Parquet，不持有目录库写事务；进度 / 冲突用短写事务落库；
3. 扫描期间上传与取数仍按旧（生效）字典工作；
4. 循环增量追赶扫描期间新发布的批次，直到一个短事务内确认「无冲突且
   批次清单未增长」才 CAS 切换生效版本——迟到核查不可能覆盖新字典；
5. 任一冲突 / 异常 / 重启遗留 => 替换失败，旧字典保持生效。
"""
import datetime
import os
import sqlite3
import threading
import time

from . import catalog, locks, standardize
from .locks import LockBusy

REPLACEMENT_LOCK = "dict_replacement"

# 测试钩子：签名 (replacement_id, event, info) -> None，生产环境为空列表
progress_hooks = []


def _now():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)


def _emit(repl_id, event, info=None):
    for hook in list(progress_hooks):
        try:
            hook(repl_id, event, info)
        except Exception:
            pass


def start_replacement(cfg, mapping):
    """创建替换任务并在后台线程执行核查。返回 replacement_id。

    调用方已持有排他 flock；这里在库内二次确认没有 scanning 行。
    字典文件在记录提交后落盘（不可变），落盘失败任务直接标失败。
    """
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            # 写事务串行化：此处检查可兜住并发请求同时通过预检的竞态
            if catalog.get_running_replacement(conn):
                raise RuntimeError("已有替换任务在执行，同一时刻只允许一次替换")
            old_version = catalog.get_active_dict_version(conn)
            now = _now()
            new_version = catalog.insert_dictionary(
                conn, dict_file="", n_mappings=len(mapping), now_iso=now.isoformat())
            dict_file = f"dict_v{new_version}.json"
            conn.execute(
                "UPDATE dictionaries SET dict_file = ? WHERE version = ?",
                (dict_file, new_version))
            total = conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
            repl_id = catalog.insert_replacement(
                conn, old_version, new_version, total, now.isoformat())
    finally:
        conn.close()

    try:
        standardize.write_dict_file(cfg.dict_dir, new_version, mapping)
    except OSError as e:
        _fail(cfg, repl_id, f"字典文件落盘失败：{e}")
        return repl_id

    thread = threading.Thread(
        target=run_replacement_safe, args=(cfg, repl_id),
        name=f"dict-replacement-{repl_id}", daemon=True)
    thread.start()
    return repl_id


def run_replacement_safe(cfg, repl_id):
    """线程入口：任何异常都保证任务有终态（failed），生效字典永不被破坏。

    数据目录已不存在（teardown / 卷卸载）时静默退出，不再尝试写库。
    """
    try:
        run_replacement(cfg, repl_id)
    except sqlite3.Error as e:
        if not os.path.isdir(cfg.data_dir):
            return
        _fail(cfg, repl_id, f"核查异常终止：{e}")
    except OSError as e:
        if not os.path.isdir(cfg.data_dir):
            return
        _fail(cfg, repl_id, f"核查异常终止：{e}")
    except Exception as e:  # noqa: BLE001 - 兜底：必须落终态
        _fail(cfg, repl_id, f"核查异常终止：{e}")


def _fail(cfg, repl_id, message):
    if not os.path.isdir(cfg.data_dir):
        return
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            row = catalog.get_replacement(conn, repl_id)
            if row and row["status"] == catalog.REPL_SCANNING:
                catalog.finish_replacement(
                    conn, repl_id, catalog.REPL_FAILED, message, _now().isoformat())
    except sqlite3.Error:
        pass  # 库不可用时进程退出即释放锁，重启恢复负责标失败
    finally:
        conn.close()


def run_replacement(cfg, repl_id):
    """核查主流程（持锁线程内执行）。"""
    conn = catalog.connect(cfg.db_path)
    try:
        repl = catalog.get_replacement(conn, repl_id)
        if not repl or repl["status"] != catalog.REPL_SCANNING:
            return
        new_version = repl["new_version"]
        old_version = repl["old_version"]
        dict_row = catalog.get_dictionary(conn, new_version)
    finally:
        conn.close()

    mapping = standardize.load_mapping(os.path.join(cfg.dict_dir, dict_row["dict_file"]))

    scanned_rid = 0
    scanned_count = 0
    while True:
        # 增量取本轮回合要核查的批次（短只读事务，扫描不占写事务）
        conn = catalog.connect(cfg.db_path)
        try:
            batch_rows = catalog.batches_for_scan(conn, after_rowid=scanned_rid)
        finally:
            conn.close()

        for b in batch_rows:
            conflicts = standardize.scan_batch_conflicts(cfg.parquet_dir, b, mapping)
            scanned_rid = b["rid"]
            scanned_count += 1
            # 每批一次短写事务刷新进度 / 冲突；扫描 Parquet 本身不持事务
            conn = catalog.connect(cfg.db_path)
            try:
                with catalog.write_txn(conn):
                    for c in conflicts:
                        catalog.add_replacement_conflict(conn, repl_id, c)
                    total_conf = catalog.count_conflicts(conn, repl_id)
                    catalog.update_replacement_progress(
                        conn, repl_id, scanned_count, total_conf, _now().isoformat())
            finally:
                conn.close()
            _emit(repl_id, "scanned",
                  {"batch_rowid": b["rid"], "batch_id": b["id"], "conflicts": len(conflicts)})

        # 静态点判定 + 切换（一个短写事务）
        conn = catalog.connect(cfg.db_path)
        try:
            with catalog.write_txn(conn):
                current = catalog.get_replacement(conn, repl_id)
                if current is None or current["status"] != catalog.REPL_SCANNING:
                    return  # 已被启动恢复等标记终态
                total_batches = conn.execute(
                    "SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
                total_conf = catalog.count_conflicts(conn, repl_id)
                if total_conf > 0:
                    catalog.finish_replacement(
                        conn, repl_id, catalog.REPL_FAILED,
                        f"核查发现 {total_conf} 处标准化冲突，保留旧字典 v{old_version}",
                        _now().isoformat())
                    _emit(repl_id, "failed", {"conflicts": total_conf})
                    return
                if total_batches > scanned_count:
                    # 核查期间有新批次发布：更新总数并继续追赶，绝不漏检
                    conn.execute(
                        "UPDATE replacements SET total_batches = ?, heartbeat_at = ?"
                        " WHERE id = ?",
                        (total_batches, _now().isoformat(), repl_id))
                    continue
                # 无冲突 + 批次清单静止 => CAS 切换；生效版本若已被改动则放弃
                active = catalog.get_active_dict_version(conn)
                if active != old_version:
                    catalog.finish_replacement(
                        conn, repl_id, catalog.REPL_FAILED,
                        f"生效字典已变为 v{active}，迟到核查放弃切换",
                        _now().isoformat())
                    _emit(repl_id, "failed", {"reason": "late"})
                    return
                if not catalog.activate_dictionary(conn, new_version, _now().isoformat()):
                    catalog.finish_replacement(
                        conn, repl_id, catalog.REPL_FAILED,
                        f"候选字典 v{new_version} 不存在，放弃切换",
                        _now().isoformat())
                    return
                catalog.finish_replacement(
                    conn, repl_id, catalog.REPL_SUCCESS, "", _now().isoformat())
                _emit(repl_id, "success", {"new_version": new_version})
                return
        finally:
            conn.close()


def request_start(cfg, mapping):
    """Web / 脚本入口：获取排他锁，启动后台替换，并在任务终态后释放锁。

    flock 是跨进程 / 跨线程的唯一仲裁：拿不到立即拒绝（同时只允许一次替换）。
    锁在后台核查生命周期内持续持有；进程退出时内核自动释放。
    """
    conn = catalog.connect(cfg.db_path)
    try:
        running = catalog.get_running_replacement(conn)
    finally:
        conn.close()
    if running:
        raise RuntimeError("已有替换任务在执行，同一时刻只允许一次替换")

    lock = locks.ExclusiveLock(cfg.lock_dir, REPLACEMENT_LOCK)
    try:
        lock.acquire()
    except locks.LockBusy:
        # 锁被占用：若库中已无 scanning，说明只是上一看门狗在收尾，短暂等待；
        # 若仍有 scanning，则是真正的替换在执行，立即拒绝。
        conn = catalog.connect(cfg.db_path)
        try:
            still_running = catalog.get_running_replacement(conn) is not None
        finally:
            conn.close()
        if still_running:
            raise
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            time.sleep(0.1)
            try:
                lock.acquire()
                break
            except locks.LockBusy:
                continue
        else:
            raise

    # 拿到锁后二次检查（持锁等待/进程重启等极端情况）
    conn = catalog.connect(cfg.db_path)
    try:
        running = catalog.get_running_replacement(conn)
    finally:
        conn.close()
    if running:
        lock.release()
        raise RuntimeError("已有替换任务在执行，同一时刻只允许一次替换")

    try:
        repl_id = start_replacement(cfg, mapping)
    except RuntimeError:
        lock.release()  # 写事务内发现并发 scanning：等价于忙，拒绝本次请求
        raise LockBusy("已有替换任务在执行，同一时刻只允许一次替换")
    except BaseException:
        lock.release()
        raise

    def _watchdog():
        # 任何异常都必须释放锁；目录消失等极端情况下兜底释放
        try:
            while os.path.isdir(cfg.data_dir):
                try:
                    conn = catalog.connect(cfg.db_path)
                    try:
                        row = catalog.get_replacement(conn, repl_id)
                    finally:
                        conn.close()
                except sqlite3.Error:
                    time.sleep(0.2)
                    continue
                if row and row["status"] != catalog.REPL_SCANNING:
                    break
                time.sleep(0.2)
        finally:
            lock.release()

    threading.Thread(target=_watchdog, name=f"repl-lock-{repl_id}",
                     daemon=True).start()
    return repl_id


def get_status(cfg, repl_id=None):
    """取替换任务状态（供进度页）。默认取最近一条。"""
    conn = catalog.connect(cfg.db_path)
    try:
        if repl_id is not None:
            repl = catalog.get_replacement(conn, repl_id)
        else:
            repl = catalog.latest_replacement(conn)
        if repl is None:
            return None
        repl = dict(repl)
        repl["conflicts"] = catalog.list_conflicts(conn, repl["id"])
        return repl
    finally:
        conn.close()


def list_history(cfg, limit=20):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.list_replacements(conn, limit)
    finally:
        conn.close()
