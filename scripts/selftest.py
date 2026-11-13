#!/usr/bin/env python3
"""端到端自检：CSV 校验、发布、结构合并、类型提升、并发、重启持久化、Web 入口。

用法（在仓库根目录，容器内或装好依赖的环境）：
    python scripts/selftest.py
"""
import datetime
import io
import os
import re
import sys
import tempfile
import threading
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import catalog, csvparse, replacement, service, standardize, store  # noqa: E402
from app.config import Config  # noqa: E402
from app.locks import ExclusiveLock, LockBusy  # noqa: E402


def check(name, cond, extra=""):
    if cond:
        print(f"  ✓ {name}")
    else:
        raise AssertionError(f"{name}  {extra}")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def make_cfg(tmp):
    cfg = Config(tmp)
    service.init_storage(cfg)
    return cfg


def snap_of(cfg, station=None):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.snapshot(conn, station)
    finally:
        conn.close()


# ---------- 1. CSV 解析与校验 ----------

def test_parser(tmp):
    p = os.path.join(tmp, "a.csv")
    write(p, "日期,麻雀,白鹭\n2024-01-01,3,x y\n2024-01-02,,5\n")
    header = csvparse.read_header(p)
    check("表头解析", header == ["日期", "麻雀", "白鹭"])
    dates, cols = csvparse.parse_rows(p, ["uint64", "text"])
    check("日期解析", str(dates[0]) == "2024-01-01")
    check("空单元格为NULL(uint64)", cols[0] == [3, None])
    check("文本列保留原文", cols[1] == ["x y", "5"])

    def expect_csv_error(name, content, types=("uint64",)):
        write(p, content)
        try:
            csvparse.parse_rows(p, list(types))
        except csvparse.CsvError:
            print(f"  ✓ {name}")
        else:
            raise AssertionError(name + " 未被拒绝")

    expect_csv_error("负数拒绝", "d,麻雀\n2024-01-01,-3\n")
    expect_csv_error("非数字拒绝", "d,麻雀\n2024-01-01,12x\n")
    expect_csv_error("超出uint64拒绝", "d,麻雀\n2024-01-01,18446744073709551616\n")
    expect_csv_error("坏日期拒绝", "d,麻雀\n2024-13-01,1\n")
    expect_csv_error("字段数不一致拒绝", "d,麻雀\n2024-01-01,1,2\n")
    expect_csv_error("无数据行拒绝", "d,麻雀\n")

    write(p, "d,麻雀\n2024-01-01,18446744073709551615\n")
    _, cols = csvparse.parse_rows(p, ["uint64"])
    check("uint64上限可用", cols[0] == [2 ** 64 - 1])

    write(p, "d,麻雀,麻雀\n2024-01-01,1,2\n")
    try:
        csvparse.read_header(p)
        raise AssertionError("重复列名未被拒绝")
    except csvparse.CsvError:
        print("  ✓ 重复列名拒绝")


# ---------- 2. 发布、结构合并、类型提升 ----------

def test_publish_merge(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "s1.csv")
    f2 = os.path.join(tmp, "s2.csv")
    f3 = os.path.join(tmp, "s3.csv")
    write(f1, "日期,麻雀,白鹭\n2023-05-01,12,3\n2023-05-02,9,\n")
    write(f2, "日期,麻雀,苍鹭\n2024-04-20,7,1\n")
    write(f3, "日期,白鹭,黑水鸡\n2023-05-01,约12,5\n")
    r1 = service.publish(cfg, "青湾", "s1.csv", f1, ["uint64", "uint64"])
    r2 = service.publish(cfg, "青湾", "s2.csv", f2, ["uint64", "uint64"])
    r3 = service.publish(cfg, "芦苇荡", "s3.csv", f3, ["text", "uint64"])
    check("新列加入", any(c["change"] == "added" and c["name"] == "苍鹭" for c in r2["changes"]))
    check("同名异型提升text", any(c["change"] == "promoted" and c["name"] == "白鹭" for c in r3["changes"]))
    check("每批独立Parquet", len({r["batch_id"] for r in (r1, r2, r3)}) == 3
          and len(os.listdir(cfg.parquet_dir)) == 3)

    snap_all = snap_of(cfg)
    snap_qw = snap_of(cfg, "青湾")
    types = {c["name"]: c["type"] for c in snap_all["columns"]}
    check("逻辑结构按列名合并", types == {"麻雀": "uint64", "白鹭": "text", "苍鹭": "uint64", "黑水鸡": "uint64"}, str(types))
    check("批次全部登记", len(snap_all["batches"]) == 3)
    check("按站点过滤", len(snap_qw["batches"]) == 2)

    schema, table = store.load_unified(cfg.parquet_dir, snap_all["columns"], snap_all["batches"])
    check("统一行数", table.num_rows == 4)
    check("统一列序", schema.names == ["obs_date", "麻雀", "白鹭", "苍鹭", "黑水鸡"])
    rows = table.to_pylist()
    check("缺列补NULL", rows[1]["苍鹭"] is None and rows[1]["白鹭"] is None)
    check("旧整数按十进制转文本", rows[0]["白鹭"] == "3" and isinstance(rows[0]["白鹭"], str))
    check("文本原样保留", rows[3]["白鹭"] == "约12")
    check("整数列保持uint64", rows[0]["麻雀"] == 12 and isinstance(rows[0]["麻雀"], int))

    # 提升后不得退回整数：再发一批把「白鹭」声明为 uint64
    f4 = os.path.join(tmp, "s4.csv")
    write(f4, "日期,白鹭\n2024-05-01,6\n")
    service.publish(cfg, "青湾", "s4.csv", f4, ["uint64"])
    snap2 = snap_of(cfg)
    types2 = {c["name"]: c["type"] for c in snap2["columns"]}
    check("提升后不退回整数", types2["白鹭"] == "text")
    _, t2 = store.load_unified(cfg.parquet_dir, snap2["columns"], snap2["batches"])
    check("新批整数也按文本读出", t2.to_pylist()[-1]["白鹭"] == "6")

    # 统一 CSV 导出
    csv_bytes = store.table_to_csv_bytes(t2).decode("utf-8")
    check("统一CSV含全部列", csv_bytes.splitlines()[0] == "obs_date,麻雀,白鹭,苍鹭,黑水鸡", csv_bytes.splitlines()[0])


# ---------- 3. 并发发布 ----------

def test_concurrent(tmp):
    cfg = make_cfg(tmp)
    errors = []

    def worker(i, decl):
        try:
            f = os.path.join(tmp, f"c{i}.csv")
            col = "共有种" if i < 2 else f"鸟种{i}"
            write(f, f"日期,{col}\n2024-06-0{i % 9 + 1},5\n")
            service.publish(cfg, f"站{i}", f"c{i}.csv", f, [decl])
        except Exception:
            errors.append(traceback.format_exc())

    # 线程 0/1 对同一列给出冲突类型(uint64/text)，其余线程各添新列
    decls = ["uint64", "text"] + ["uint64"] * 8
    threads = [threading.Thread(target=worker, args=(i, decls[i])) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check("并发无异常", not errors, "\n".join(errors))
    snap = snap_of(cfg)
    types = {c["name"]: c["type"] for c in snap["columns"]}
    check("并发批次全保留", len(snap["batches"]) == 10, str(len(snap["batches"])))
    check("并发新增列全保留", all(f"鸟种{i}" in types for i in range(2, 10)), str(types))
    check("并发下同列提升为text", types.get("共有种") == "text", str(types.get("共有种")))
    _, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
    check("并发统一行数", table.num_rows == 10)
    vals = sorted(r["共有种"] for r in table.to_pylist() if r["共有种"] is not None)
    check("冲突列两批数据均保留为文本", vals == ["5", "5"], str(vals))


# ---------- 4. 坏批不发布 ----------

def test_bad_batch(tmp):
    cfg = make_cfg(tmp)
    good = os.path.join(tmp, "g.csv")
    write(good, "日期,麻雀\n2024-01-01,3\n")
    service.publish(cfg, "站A", "g.csv", good, ["uint64"])
    before_files = set(os.listdir(cfg.parquet_dir))

    bad = os.path.join(tmp, "b.csv")
    write(bad, "日期,麻雀,白鹭\n2024-01-02,abc,1\n")
    try:
        service.publish(cfg, "站A", "b.csv", bad, ["uint64", "uint64"])
        raise AssertionError("坏整数应整批拒绝")
    except csvparse.CsvError:
        print("  ✓ 坏整数整批拒绝")
    check("坏批未登记", len(snap_of(cfg)["batches"]) == 1)
    check("坏批无残件", set(os.listdir(cfg.parquet_dir)) == before_files)

    # 写盘失败（模拟）=> 整批不发布
    orig = store.write_parquet_atomic
    def boom(_table, _path):
        raise OSError("disk full (simulated)")
    store.write_parquet_atomic = boom
    try:
        try:
            service.publish(cfg, "站A", "g2.csv", good, ["uint64"])
            raise AssertionError("写盘失败应抛错")
        except OSError:
            print("  ✓ 写盘失败整批不发布")
    finally:
        store.write_parquet_atomic = orig
    check("写盘失败未登记", len(snap_of(cfg)["batches"]) == 1)
    check("写盘失败无残件", set(os.listdir(cfg.parquet_dir)) == before_files)


# ---------- 5. 重启持久化 ----------

def test_restart(tmp):
    cfg = make_cfg(tmp)
    f = os.path.join(tmp, "r.csv")
    write(f, "日期,麻雀\n2024-01-01,3\n")
    service.publish(cfg, "站A", "r.csv", f, ["uint64"])
    cfg2 = Config(cfg.data_dir)          # 模拟重启：全新对象指向同一数据目录
    service.init_storage(cfg2)
    snap = snap_of(cfg2)
    check("重启后批次在", len(snap["batches"]) == 1)
    _, table = store.load_unified(cfg2.parquet_dir, snap["columns"], snap["batches"])
    check("重启后数据可读", table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 3)
    check("目录不引用残件", all(
        os.path.isfile(os.path.join(cfg2.parquet_dir, b["parquet_file"]))
        for b in snap["batches"]))


# ---------- 6. Web 入口 ----------

def test_web(tmp):
    os.environ["DATA_DIR"] = os.path.join(tmp, "webdata")
    from app.web import create_app
    app = create_app()
    app.config["TESTING"] = True
    c = app.test_client()

    r = c.get("/")
    check("首页200", r.status_code == 200)

    data = {"station": "青湾",
            "file": (io.BytesIO("日期,麻雀,白鹭\n2023-05-01,12,3\n".encode("utf-8")), "a.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    check("上传后进入声明页", r.status_code == 200 and "逐列声明类型".encode() in r.data)
    m = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data)
    check("声明页含暂存令牌", m is not None)
    token = m.group(1).decode()

    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "a.csv",
                                 "type_0": "uint64", "type_1": "text"})
    check("发布后跳转预览", r.status_code == 302 and "/preview" in r.headers["Location"])

    r = c.get("/preview?station=青湾")
    check("预览含数据", r.status_code == 200 and "麻雀".encode() in r.data and b"12" in r.data)

    r = c.get("/download.csv?station=青湾")
    body = r.data.decode("utf-8")
    check("下载统一CSV", r.status_code == 200 and r.headers["Content-Type"].startswith("text/csv"))
    check("CSV含表头与数据", "obs_date" in body and "麻雀" in body and "12" in body)

    # 坏整数批次：网页流程下整批不发布
    data = {"station": "青湾",
            "file": (io.BytesIO("日期,麻雀\n2023-05-02,oops\n".encode("utf-8")), "bad.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "bad.csv",
                                 "type_0": "uint64"}, follow_redirects=True)
    check("坏批提示未发布", "批次未发布".encode() in r.data)
    check("坏批未入统一结构", b"oops" not in c.get("/download.csv").data)


def wait_terminal(cfg, repl_id, timeout=30):
    """轮询等待替换任务进入终态。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = replacement.get_status(cfg, repl_id)
        if st and st["status"] != catalog.REPL_SCANNING:
            return st
        time.sleep(0.05)
    raise AssertionError(f"替换任务 {repl_id} 超时未结束")


def wait_lock_free(cfg, timeout=5):
    from app import locks
    deadline = time.monotonic() + timeout
    while locks.is_locked(cfg.lock_dir, replacement.REPLACEMENT_LOCK) \
            and time.monotonic() < deadline:
        time.sleep(0.05)
    return not locks.is_locked(cfg.lock_dir, replacement.REPLACEMENT_LOCK)


def active_version(cfg):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.get_active_dict_version(conn)
    finally:
        conn.close()


def load_std(cfg, station=None):
    snap = snap_of(cfg, station)
    conn = catalog.connect(cfg.db_path)
    try:
        drow = catalog.get_dictionary(conn, snap["active_dict_version"])
    finally:
        conn.close()
    mapping = standardize.load_mapping(os.path.join(cfg.dict_dir, drow["dict_file"]))
    return standardize.load_standardized(
        cfg.parquet_dir, snap["columns"], snap["batches"], mapping)


# ---------- 7. 标准化核心规则（纯逻辑） ----------

def test_standardize_core(tmp):
    # 单层不递归：A->B, B->C 时 A 只到 B；未收录保留原名
    mapping = {"家雀": "麻雀", "麻雀": "树麻雀", "白鹭": "小白鹭"}
    names = ["家雀", "麻雀", "苍鹭"]
    check("单层映射不递归", standardize.standard_name("家雀", mapping) == "麻雀")
    check("未收录保留原名", standardize.standard_name("苍鹭", mapping) == "苍鹭")
    order = standardize.standard_order(
        [{"name": n} for n in names], mapping)
    check("标准列序按原名首次出现", order == ["麻雀", "树麻雀", "苍鹭"], str(order))

    # 冲突场景：家雀与麻雀映到同一标准名「麻雀」（单层，不经过树麻雀）
    merge_map = {"家雀": "麻雀", "麻雀": "麻雀"}
    rows = [
        {"家雀": 3, "麻雀": 3, "苍鹭": None},      # 同值合一，整数转十进制文本
        {"家雀": None, "麻雀": None, "苍鹭": 5},   # 全空 -> NULL
        {"家雀": 3, "麻雀": 4, "苍鹭": "x"},       # 异值冲突
        {"家雀": "3", "麻雀": 3, "苍鹭": None},    # 文本"3"与整数3等值
    ]
    std_names, std_rows, conflicts = standardize.standardize_rows(names, rows, merge_map)
    check("标准名集合", std_names == ["麻雀", "苍鹭"], str(std_names))
    check("同值合一且整数转文本", std_rows[0]["麻雀"] == "3" and isinstance(std_rows[0]["麻雀"], str))
    check("全空仍为NULL", std_rows[1]["麻雀"] is None)
    check("异值报冲突（行号从1起）",
          len(conflicts) == 1 and conflicts[0]["row_number"] == 3
          and conflicts[0]["standard_name"] == "麻雀"
          and conflicts[0]["original_value"] == "4", str(conflicts))
    check("冲突位置None", std_rows[2]["麻雀"] is None)
    check("文本与整数等值不冲突", std_rows[3]["麻雀"] == "3")

    # 字典解析：CSV 风格 / JSON / 非法输入
    m1 = standardize.parse_mapping_text("# 注释\n家雀,麻雀\n 白鹭 , 小白鹭 \n")
    check("CSV风格字典解析", m1 == {"家雀": "麻雀", "白鹭": "小白鹭"})
    m2 = standardize.parse_mapping_text('{"家雀": "麻雀"}')
    check("JSON字典解析", m2 == {"家雀": "麻雀"})

    def expect_dict_error(name, text):
        try:
            standardize.parse_mapping_text(text)
        except standardize.DictError:
            print(f"  ✓ {name}")
        else:
            raise AssertionError(name + " 未被拒绝")

    expect_dict_error("空字典拒绝", "")
    expect_dict_error("坏行拒绝", "家雀,麻雀,多余")
    expect_dict_error("坏JSON拒绝", "{a:}")
    expect_dict_error("一键多值拒绝", "家雀,麻雀\n家雀,树麻雀")
    expect_dict_error("保留列名拒绝", f"x,{store.DATE_COLUMN}")

    # 发布前校验：冲突批次拒绝
    try:
        standardize.validate_publish(names, rows, merge_map)
        raise AssertionError("冲突批次应被拒绝")
    except standardize.DictError:
        print("  ✓ 发布前标准化冲突整批拒绝")
    standardize.validate_publish(names, rows[:2], merge_map)  # 无冲突不抛
    print("  ✓ 无冲突批次通过校验")


# ---------- 8. 替换成功：后台核查 + 切换 + 标准化取数 ----------

def test_replacement_success(tmp):
    cfg = make_cfg(tmp)
    check("初始生效字典为v1", active_version(cfg) == 1)

    f1 = os.path.join(tmp, "a.csv")
    f2 = os.path.join(tmp, "b.csv")
    write(f1, "日期,家雀,麻雀\n2024-01-01,3,3\n2024-01-02,,5\n")
    write(f2, "日期,麻雀,苍鹭\n2024-02-01,7,1\n")
    service.publish(cfg, "青湾", "a.csv", f1, ["uint64", "uint64"])
    service.publish(cfg, "青湾", "b.csv", f2, ["uint64", "uint64"])

    # 原统一结构保留原名
    snap = snap_of(cfg)
    _sch, raw = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
    check("原结构列名不变", raw.schema.names == ["obs_date", "家雀", "麻雀", "苍鹭"])
    check("原Parquet文件数不变", len(os.listdir(cfg.parquet_dir)) == 2)

    events = []
    replacement.progress_hooks.append(lambda rid, ev, info: events.append((ev, info)))
    try:
        rid = replacement.request_start(cfg, {"家雀": "麻雀", "苍鹭": "苍鹭"})
        st = wait_terminal(cfg, rid)
    finally:
        replacement.progress_hooks.clear()
    check("替换成功", st["status"] == catalog.REPL_SUCCESS, str(st))
    check("生效字典切换到v2", active_version(cfg) == 2)
    check("进度覆盖全部批次", st["progress_batches"] == 2 and st["total_batches"] == 2)
    check("无冲突", st["n_conflicts"] == 0)
    check("扫描事件逐批发出", len([e for e in events if e[0] == "scanned"]) == 2, str(events))
    check("发出成功事件", any(e[0] == "success" for e in events), str(events))
    check("替换锁事后已释放", wait_lock_free(cfg))

    # 标准化取数：家雀/麻雀合一，苍鹭恒等映射保留
    schema, table = load_std(cfg)
    check("标准化列序", schema.names == ["obs_date", "麻雀", "苍鹭"], str(schema.names))
    check("标准化列均为文本", str(table.schema.field("麻雀").type) == "string")
    rows = table.to_pylist()
    check("同值合一行1", rows[0]["麻雀"] == "3")
    check("全空NULL行2（家雀空、麻雀5）", rows[1]["麻雀"] == "5" and rows[1]["苍鹭"] is None)
    check("恒等映射保留苍鹭", rows[2]["苍鹭"] == "1")

    # 标准化 CSV
    csv_text = store.table_to_csv_bytes(table).decode("utf-8")
    check("标准化CSV表头", csv_text.splitlines()[0] == "obs_date,麻雀,苍鹭",
          csv_text.splitlines()[0])

    # 新批次按新字典发布，登记 dict_version
    f3 = os.path.join(tmp, "c.csv")
    write(f3, "日期,家雀\n2024-03-01,9\n")
    service.publish(cfg, "青湾", "c.csv", f3, ["uint64"])
    snap2 = snap_of(cfg)
    check("新批记录生效字典版本", snap2["batches"][-1]["dict_version"] == 2)
    _s, t3 = load_std(cfg)
    check("新批按新字典标准化", t3.to_pylist()[-1]["麻雀"] == "9")


# ---------- 9. 替换失败：冲突保留旧字典，明细列批次/行号/原值 ----------

def test_replacement_conflict(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "a.csv")
    write(f1, "日期,家雀,麻雀\n2024-01-01,3,4\n2024-01-02,5,5\n")
    service.publish(cfg, "青湾", "a.csv", f1, ["uint64", "uint64"])

    rid = replacement.request_start(cfg, {"家雀": "麻雀"})
    st = wait_terminal(cfg, rid)
    check("冲突替换失败", st["status"] == catalog.REPL_FAILED)
    check("保留旧字典v1", active_version(cfg) == 1)
    check("冲突数为1", st["n_conflicts"] == 1, str(st["n_conflicts"]))
    c = st["conflicts"][0]
    check("冲突行号从1起", c["row_number"] == 1, str(c))
    check("冲突含批次/站点/原值",
          c["batch_id"] and c["station"] == "青湾"
          and c["original_name"] == "麻雀" and c["original_value"] == "4"
          and c["standard_name"] == "麻雀", str(c))
    check("锁已释放可再发起", wait_lock_free(cfg))

    # 旧字典下标准化取数仍按原名（空字典 => 全部保留原名）
    schema, table = load_std(cfg)
    check("失败后取数沿用原名", schema.names == ["obs_date", "家雀", "麻雀"], str(schema.names))

    # 修订字典（去掉冲突映射）后可成功；失败候选 v2 保留为历史版本，新版本号为 v3
    rid2 = replacement.request_start(cfg, {"苍鹭": "苍鹭"})  # 无实际别名合并，恒等
    st2 = wait_terminal(cfg, rid2)
    check("无冲突再次替换成功", st2["status"] == catalog.REPL_SUCCESS
          and st2["new_version"] == 3 and active_version(cfg) == 3, str(st2))


# ---------- 10. 核查期间新批次不漏检；发布按生效字典整批校验 ----------

def test_replacement_concurrent_publish(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "a.csv")
    write(f1, "日期,家雀\n2024-01-01,3\n")
    service.publish(cfg, "青湾", "a.csv", f1, ["uint64"])

    # 10a) 替换核查期间发布的新批次必须被候选字典追赶扫描到（不漏检）
    gate = threading.Event()       # 卡住替换线程，确保新批次在静态点判定前入湖
    started = threading.Event()

    def hook(rid, ev, info):
        if ev == "scanned":
            started.set()
            gate.wait(5)

    replacement.progress_hooks.append(hook)
    try:
        # 候选字典：家雀、麻雀 -> 麻雀。第一批只有家雀，可顺利扫完
        rid = replacement.request_start(cfg, {"家雀": "麻雀", "麻雀": "麻雀"})
        started.wait(5)  # 等第一批扫描完成、替换线程停在静态点之前
        # 扫描期间按旧字典（v1 空映射）发布：发布本身不冲突，允许入湖
        f2 = os.path.join(tmp, "b.csv")
        write(f2, "日期,家雀,麻雀\n2024-02-01,3,9\n")  # 候选字典下同标准名异值
        service.publish(cfg, "青湾", "b.csv", f2, ["uint64", "uint64"])
        gate.set()  # 放行替换线程：它必须在静态点发现新批次并追赶扫描
        replacement.progress_hooks.remove(hook)  # 后续替换不再受此钩子影响
    finally:
        replacement.progress_hooks.clear()

    st = wait_terminal(cfg, rid)
    check("新批次被追赶扫描到并判冲突", st["status"] == catalog.REPL_FAILED, str(st))
    check("进度覆盖两个批次", st["progress_batches"] == 2, str(st["progress_batches"]))
    check("冲突指向新批次且行号正确",
          st["n_conflicts"] >= 1 and st["conflicts"][0]["row_number"] == 1
          and st["conflicts"][0]["standard_name"] == "麻雀", str(st["conflicts"]))
    check("失败后生效字典仍是v1（扫描期按旧字典取数）", active_version(cfg) == 1)

    # 10b) 成功切换后，发布按新生效字典整批校验，冲突不发布、不留残件
    d2 = os.path.join(tmp, "sub2")
    cfg2 = make_cfg(d2)
    g1 = os.path.join(d2, "g1.csv")
    write(g1, "日期,家雀\n2024-03-01,2\n")
    service.publish(cfg2, "青湾", "g1.csv", g1, ["uint64"])
    rid_ok = replacement.request_start(cfg2, {"家雀": "麻雀", "麻雀": "麻雀"})
    st_ok = wait_terminal(cfg2, rid_ok)
    check("合并字典替换成功", st_ok["status"] == catalog.REPL_SUCCESS
          and active_version(cfg2) == 2, str(st_ok))

    g2 = os.path.join(d2, "g2.csv")
    write(g2, "日期,家雀,麻雀\n2024-04-01,3,9\n")  # v2 下映到同一标准名且异值
    before = len(snap_of(cfg2)["batches"])
    before_files = set(os.listdir(cfg2.parquet_dir))
    try:
        service.publish(cfg2, "青湾", "g2.csv", g2, ["uint64", "uint64"])
        raise AssertionError("生效字典下的冲突批次应被拒绝")
    except standardize.DictError:
        print("  ✓ 生效字典下冲突批次整批不发布")
    check("冲突批次未登记", len(snap_of(cfg2)["batches"]) == before)
    check("冲突批次无残件", set(os.listdir(cfg2.parquet_dir)) == before_files)


# ---------- 11. 同时只允许一次替换（跨进程锁语义） ----------

def test_replacement_lock(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "a.csv")
    write(f1, "日期,家雀\n2024-01-01,3\n")
    service.publish(cfg, "青湾", "a.csv", f1, ["uint64"])

    lock = ExclusiveLock(cfg.lock_dir, replacement.REPLACEMENT_LOCK)
    lock.acquire()
    try:
        try:
            replacement.request_start(cfg, {"家雀": "麻雀"})
            raise AssertionError("锁占用时应拒绝新替换")
        except LockBusy:
            print("  ✓ 锁占用时第二次替换被拒绝")
    finally:
        lock.release()

    # 库中存在 scanning 行时立即拒绝（不等锁），保证同时只允许一次替换
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
            newv = catalog.insert_dictionary(conn, "dict_vX.json", 1, now)
            scan_id = catalog.insert_replacement(conn, 1, newv, 1, now)
    finally:
        conn.close()
    try:
        replacement.request_start(cfg, {"家雀": "麻雀"})
        raise AssertionError("存在 scanning 任务时应拒绝新替换")
    except RuntimeError as e:
        check("scanning 存在时拒绝", "只允许一次" in str(e))
    # 清理占位 scanning
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            catalog.finish_replacement(
                conn, scan_id, catalog.REPL_FAILED, "测试清理",
                datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"))
    finally:
        conn.close()

    # 正常路径：替换仍可成功
    rid = replacement.request_start(cfg, {"家雀": "麻雀"})
    st = wait_terminal(cfg, rid)
    check("唯一替换成功", st["status"] == catalog.REPL_SUCCESS)
    check("替换锁事后释放", wait_lock_free(cfg))


# ---------- 12. 重启恢复：未完成替换标失败，生效字典保留 ----------

def test_replacement_restart(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "a.csv")
    write(f1, "日期,家雀\n2024-01-01,3\n")
    service.publish(cfg, "青湾", "a.csv", f1, ["uint64"])

    # 手工插入一条心跳过期的 scanning 记录（模拟进程被杀）
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=999)
            newv = catalog.insert_dictionary(conn, "dict_v2.json", 1, old.isoformat())
            rid = catalog.insert_replacement(conn, 1, newv, 1, old.isoformat())
    finally:
        conn.close()
    check("重启前生效仍为v1", active_version(cfg) == 1)

    cfg2 = Config(cfg.data_dir)
    recovered = service.init_storage(cfg2)
    check("启动恢复返回遗留任务", recovered == [rid], str(recovered))
    st = replacement.get_status(cfg2, rid)
    check("遗留替换标记失败", st["status"] == catalog.REPL_FAILED and st["finished_at"])
    check("生效字典保留v1", active_version(cfg2) == 1)

    # 新鲜心跳的 scanning（其他进程正在跑）不得被误杀
    conn = catalog.connect(cfg2.db_path)
    try:
        with catalog.write_txn(conn):
            now = datetime.datetime.now(datetime.timezone.utc)
            newv = catalog.insert_dictionary(conn, "dict_v3.json", 1, now.isoformat())
            rid2 = catalog.insert_replacement(conn, 1, newv, 1, now.isoformat())
    finally:
        conn.close()
    service.init_storage(Config(cfg.data_dir))
    st2 = replacement.get_status(cfg, rid2)
    check("新鲜心跳任务不被误杀", st2["status"] == catalog.REPL_SCANNING)
    # 清理：手工终态，避免污染后续
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            catalog.finish_replacement(conn, rid2, catalog.REPL_FAILED,
                                       "测试清理", datetime.datetime.now(datetime.timezone.utc)
                                       .isoformat(timespec="seconds"))
    finally:
        conn.close()


# ---------- 13. 标准化 Web 入口 ----------

def test_standard_web(tmp):
    os.environ["DATA_DIR"] = os.path.join(tmp, "webdata2")
    from app.web import create_app
    app = create_app()
    app.config["TESTING"] = True
    c = app.test_client()

    data = {"station": "青湾",
            "file": (io.BytesIO("日期,家雀,麻雀\n2024-01-01,3,3\n".encode("utf-8")), "a.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "a.csv",
                                 "type_0": "uint64", "type_1": "uint64"})
    check("发布跳转", r.status_code == 302)

    # v1（空字典）下再发布一批异值数据：当时不冲突，留给后续字典制造历史冲突
    data = {"station": "青湾",
            "file": (io.BytesIO("日期,家雀,麻雀\n2024-01-02,8,9\n".encode("utf-8")), "h.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "h.csv",
                                 "type_0": "uint64", "type_1": "uint64"})
    check("v1下异值两列可发布", r.status_code == 302)

    r = c.get("/standard")
    check("字典页200且含v1", r.status_code == 200 and "生效字典 v1".encode() in r.data)

    # 成功替换（只合并家雀->麻雀；h.csv 两列此时在 v2 下立即冲突，
    # 故先用仅含 a.csv 同值的映射验证成功路径——改用单列映射避开 h.csv）
    r = c.post("/standard/replace",
               data={"mapping": "苍鹭,苍鹭\n"}, follow_redirects=True)
    check("提交后进入任务页", r.status_code == 200 and "字典替换任务".encode() in r.data)
    import time as _t
    for _ in range(100):
        r = c.get("/standard")
        if "核查中".encode() not in r.data:
            break
        _t.sleep(0.1)
    check("页面显示已切换", "已切换".encode() in r.data and "v2".encode() in r.data)

    # 再用真正的别名合并字典（家雀->麻雀）：h.csv 历史异值 => 必须失败
    r = c.post("/standard/replace", data={"mapping": "家雀,麻雀\n麻雀,麻雀\n"})
    loc = r.headers["Location"]
    check("合并替换进入任务详情页", "/standard/replacement/" in loc, loc)
    for _ in range(100):
        r = c.get(loc)
        if "核查中".encode() not in r.data:
            break
        _t.sleep(0.1)
    check("历史冲突任务页展示失败", "失败".encode() in r.data)
    check("冲突明细展示批次/行号/原值",
          "数据行号".encode() in r.data and "麻雀".encode() in r.data and b"9" in r.data
          and b"h.csv" in r.data)
    check("失败后生效字典仍为v2", "生效字典 v2".encode() in c.get("/standard").data)

    # 标准化预览：v2 为恒等字典，原名全部保留
    r = c.get("/standard/preview")
    check("标准化预览200", r.status_code == 200 and "家雀".encode() in r.data)
    r = c.get("/standard/download.csv")
    body = r.data.decode("utf-8")
    check("标准化CSV下载", r.status_code == 200
          and body.splitlines()[0] == "obs_date,家雀,麻雀", body.splitlines()[0])

    # 发布期整批校验：构造一个 v2 下的冲突（v2 含 苍鹭->苍鹭；先加一列再合并不便，
    # 直接验证文本/整数列在同标准名下的异值拒绝：用两个映到同名的列）
    # 先切换一个干净的合并字典（仅 a.csv 的同值能过；h.csv 会挡，故用新站点隔离）
    data = {"station": "新岛站",
            "file": (io.BytesIO("日期,家雀,麻雀\n2024-03-01,5,5\n".encode("utf-8")), "n.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
    c.post("/publish", data={"token": token, "station": "新岛站", "filename": "n.csv",
                             "type_0": "uint64", "type_1": "uint64"})
    # 全库替换仍会被青湾 h.csv 挡住，验证冲突清单里包含两个站点的批次
    r = c.post("/standard/replace", data={"mapping": "家雀,麻雀\n"})
    loc = r.headers["Location"]
    for _ in range(100):
        r = c.get(loc)
        if "核查中".encode() not in r.data:
            break
        _t.sleep(0.1)
    check("跨站点核查失败并保留v2", "失败".encode() in r.data
          and "生效字典 v2".encode() in c.get("/standard").data)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        steps = [
            ("CSV 解析与校验", test_parser),
            ("发布 / 结构合并 / 类型提升", test_publish_merge),
            ("并发发布（新增列 + 同列类型冲突）", test_concurrent),
            ("坏整数 / 写盘失败整批不发布", test_bad_batch),
            ("重启后已发布数据可用", test_restart),
            ("Web 入口（Flask test client）", test_web),
            ("标准化核心规则（单层映射/合一/冲突/十进制文本）", test_standardize_core),
            ("替换成功：后台核查 + 切换 + 标准化取数", test_replacement_success),
            ("替换失败：冲突保留旧字典并出明细", test_replacement_conflict),
            ("核查期间新批次不漏检 + 发布整批校验", test_replacement_concurrent_publish),
            ("替换跨进程互斥（同时只允许一次）", test_replacement_lock),
            ("重启恢复：未完成替换标失败、生效字典保留", test_replacement_restart),
            ("标准化 Web 入口", test_standard_web),
        ]
        for i, (title, fn) in enumerate(steps, 1):
            print(f"[{i}/{len(steps)}] {title}")
            fn(os.path.join(tmp, f"case{i}"))
    print("\n全部自检通过 ✅")


if __name__ == "__main__":
    main()
