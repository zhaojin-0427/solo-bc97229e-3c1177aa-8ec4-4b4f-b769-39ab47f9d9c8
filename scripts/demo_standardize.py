#!/usr/bin/env python3
"""鸟种别名标准化演示（配合 seed_demo.py 使用）。

流程：
1. 用 samples/demo_dict_success.json 发起替换：家雀 -> 麻雀，核查无冲突后切换；
2. 用 samples/demo_dict_conflict.json 发起替换：夜鹭/苍鹭 -> 鹭类，
   历史异值导致冲突，替换失败并保留旧字典，页面/终端列出冲突批次与行号；
3. 打印当前生效字典下的标准化取数结果。

用法（容器内）：
    python scripts/seed_demo.py
    python scripts/demo_standardize.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import catalog, replacement, standardize, store  # noqa: E402
from app.config import Config  # noqa: E402


def wait(cfg, repl_id, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = replacement.get_status(cfg, repl_id)
        if st and st["status"] != catalog.REPL_SCANNING:
            return st
        time.sleep(0.2)
    raise SystemExit(f"替换 {repl_id} 超时")


def main():
    cfg = Config()
    service_init = __import__("app.service", fromlist=["init_storage"])
    service_init.init_storage(cfg)
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # 1) 无冲突替换
    with open(os.path.join(root, "samples", "demo_dict_success.json"), encoding="utf-8") as f:
        ok_mapping = json.load(f)
    rid = replacement.request_start(cfg, ok_mapping)
    st = wait(cfg, rid)
    print(f"[1] 成功字典替换 #{rid}: {st['status']} "
          f"v{st['old_version']} -> v{st['new_version']}，冲突 {st['n_conflicts']} 处")

    # 2) 冲突替换
    with open(os.path.join(root, "samples", "demo_dict_conflict.json"), encoding="utf-8") as f:
        bad_mapping = json.load(f)
    rid2 = replacement.request_start(cfg, bad_mapping)
    st2 = wait(cfg, rid2)
    print(f"[2] 冲突字典替换 #{rid2}: {st2['status']}（{st2['error']}）")
    for c in st2["conflicts"]:
        print(f"    冲突: 批次={c['batch_id'][:8]} 站点={c['station']} "
              f"文件={c['source_filename']} 数据行号={c['row_number']} "
              f"标准名={c['standard_name']} 原名={c['original_name']} 原值={c['original_value']}")

    # 3) 生效字典下的标准化取数
    conn = catalog.connect(cfg.db_path)
    try:
        snap = catalog.snapshot(conn)
        drow = catalog.get_dictionary(conn, snap["active_dict_version"])
    finally:
        conn.close()
    mapping = standardize.load_mapping(os.path.join(cfg.dict_dir, drow["dict_file"]))
    schema, table = standardize.load_standardized(
        cfg.parquet_dir, snap["columns"], snap["batches"], mapping)
    print(f"[3] 生效字典 v{snap['active_dict_version']} 标准化列: {schema.names}")
    csv_text = store.table_to_csv_bytes(table).decode("utf-8")
    print("标准化 CSV 前 6 行：")
    for line in csv_text.splitlines()[:6]:
        print("   ", line)


if __name__ == "__main__":
    main()
