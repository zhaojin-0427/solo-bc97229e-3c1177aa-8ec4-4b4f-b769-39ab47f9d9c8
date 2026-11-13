"""鸟种别名标准化核心（纯逻辑 + 文件读写，不碰目录库写事务）。

规则（需求约定）：
- 字典为「原名 -> 标准名」的单层映射，只映射一次，绝不递归；
  未收录的原名保留原名。
- 每条调查（批次）独立处理，不跨批次合值。
- 整数统一按十进制转成文本后再比较 / 输出；空值一律为 NULL。
- 同一批次内映射到同一标准名的多个非空值：完全相同才合一；
  全为空（含 NULL）结果仍为 NULL；出现不同非空值即为冲突。
- 原 Parquet 与原统一结构保持不变；标准化在读取时按生效字典即时计算。
"""
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

from . import store


class DictError(ValueError):
    """字典内容不合法。"""


# ---------- 字典解析与持久化 ----------

def parse_mapping_text(text):
    """接受两种格式：
    1) 每行「原名,标准名」（# 开头为注释，空行跳过）；
    2) JSON 对象 {"原名": "标准名", ...}。
    返回有序 dict（去空白后的键值）。原名 -> 原名 的恒等映射允许但无效果。
    """
    raw = (text or "").strip()
    if not raw:
        raise DictError("字典为空：请至少提供一行「原名,标准名」")
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            raise DictError(f"JSON 解析失败：{e}")
        if not isinstance(obj, dict):
            raise DictError("JSON 字典必须是 {原名: 标准名} 对象")
        items = obj.items()
    else:
        items = []
        for lineno, line in enumerate(raw.splitlines(), 1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            parts = [p.strip() for p in s.split(",")]
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise DictError(f"第 {lineno} 行格式错误，应为「原名,标准名」：{line!r}")
            items.append(tuple(parts))
    mapping = {}
    for k, v in items:
        k = (k or "").strip()
        v = (v or "").strip()
        if not k or not v:
            raise DictError("字典存在空的原名或标准名")
        if k == store.DATE_COLUMN or v == store.DATE_COLUMN:
            raise DictError(f"字典不能使用保留列名 {store.DATE_COLUMN}")
        if k in mapping and mapping[k] != v:
            raise DictError(f"原名 {k!r} 对应了多个标准名")
        mapping[k] = v
    if not mapping:
        raise DictError("字典没有有效映射")
    return mapping


def write_dict_file(dict_dir, version, mapping):
    """不可变字典文件：dicts/dict_v<N>.json，临时文件 + 原子 rename。"""
    os.makedirs(dict_dir, exist_ok=True)
    path = os.path.join(dict_dir, f"dict_v{version}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return path


def load_mapping(dict_path):
    with open(dict_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise DictError(f"字典文件 {dict_path} 内容损坏")
    return {str(k): str(v) for k, v in obj.items()}


def standard_name(name, mapping):
    """单层映射：只查一次，未收录保留原名。"""
    return mapping.get(name, name)


# ---------- 逐批标准化与冲突检测 ----------

def _cell_text(value):
    """整数 -> 十进制文本；None -> None；文本原样（空串视为 NULL）。"""
    if value is None:
        return None
    if isinstance(value, bool):  # 防御：bool 是 int 子类
        value = int(value)
    if isinstance(value, int):
        return str(value)
    s = str(value)
    return s if s != "" else None


def standard_order(original_columns, mapping):
    """按目录原列序生成标准列序：原名首次出现的位置为准，去重保序。"""
    order = []
    seen = set()
    for c in original_columns:
        std = standard_name(c["name"], mapping)
        if std not in seen:
            seen.add(std)
            order.append(std)
    return order


def standardize_rows(column_names, rows, mapping):
    """把一个批次的行（list[dict]，键为原列名）标准化。

    返回 (std_names, std_rows, conflicts)：
    - 每个批次独立处理；标准列全部为文本；
    - conflicts: [{row_number(从1起), standard_name, original_name, original_value}, ...]
      冲突行该标准名置 None（不得猜测取值）。
    """
    std_names = []
    name_to_std = {}
    for name in column_names:
        std = standard_name(name, mapping)
        name_to_std[name] = std
        if std not in std_names:
            std_names.append(std)

    std_rows = []
    conflicts = []
    for idx, row in enumerate(rows, start=1):
        # standard -> {"value": 文本或None}；多个来源共用同一聚合槽
        agg = {s: {"value": None} for s in std_names}
        row_conflicts = set()  # 本行已发生异值冲突的标准名
        for name in column_names:
            std = name_to_std[name]
            txt = _cell_text(row.get(name))
            if txt is None:
                continue
            slot = agg[std]
            if slot["value"] is None:
                slot["value"] = txt
            elif slot["value"] != txt:
                # 异值冲突：记录每个与已取值不同的非空来源
                row_conflicts.add(std)
                conflicts.append({
                    "row_number": idx,
                    "standard_name": std,
                    "original_name": name,
                    "original_value": txt,
                })
        out = {}
        for std in std_names:
            # 任一来源发生异值 => 该行该标准名置 None（冲突明细另行落库）
            out[std] = None if std in row_conflicts else agg[std]["value"]
        std_rows.append(out)
    return std_names, std_rows, conflicts


def scan_batch_conflicts(parquet_dir, batch, mapping):
    """只读扫描一个已发布批次，返回冲突明细（附带批次信息），不持有目录库事务。"""
    path = os.path.join(parquet_dir, batch["parquet_file"])
    table = pq.read_table(path)
    names = [n for n in table.schema.names if n != store.DATE_COLUMN]
    rows = table.select(names).to_pylist()
    _std_names, _std_rows, conflicts = standardize_rows(names, rows, mapping)
    for c in conflicts:
        c["batch_id"] = batch["id"]
        c["station"] = batch["station"]
        c["source_filename"] = batch["source_filename"]
    return conflicts


def validate_publish(column_names, rows, mapping):
    """发布前整批校验（按提交时生效字典）。冲突即抛 DictError，整批不发布。"""
    _std_names, _std_rows, conflicts = standardize_rows(column_names, rows, mapping)
    if conflicts:
        c = conflicts[0]
        raise DictError(
            f"标准化冲突（第 {c['row_number']} 行，标准名「{c['standard_name']}」"
            f"出现不同非空值，原值 {c['original_value']!r}）；整批不发布，共 {len(conflicts)} 处冲突")


# ---------- 标准化取数（保留原 Parquet / 原统一结构） ----------

def load_standardized(parquet_dir, columns, batches, mapping):
    """按同一份（逻辑结构, 批次清单, 生效字典）快照即时计算标准化表。

    每个批次独立标准化后纵向拼接；标准列均为 string，obs_date 保持 date32。
    缺标准列补 NULL。返回 (std_schema, pa.Table)。
    """
    std_order = standard_order(columns, mapping)
    schema = pa.schema(
        [pa.field(store.DATE_COLUMN, pa.date32())]
        + [pa.field(s, pa.string()) for s in std_order])
    tables = []
    for b in batches:
        table = pq.read_table(os.path.join(parquet_dir, b["parquet_file"]))
        names = [n for n in table.schema.names if n != store.DATE_COLUMN]
        rows = table.to_pylist()
        s_names, s_rows, conflicts = standardize_rows(names, rows, mapping)
        if conflicts:  # 已发布数据只可能在字典切换后产生冲突；读取不静默合并
            raise DictError(
                f"批次 {b['id'][:8]}（{b['station']}）在当前字典下存在 "
                f"{len(conflicts)} 处标准化冲突，请先处理后再取数")
        arrays = [table.column(store.DATE_COLUMN)]
        by_name = {n: [r.get(n) for r in s_rows] for n in s_names}
        for std in std_order:
            if std in by_name:
                arrays.append(pa.array(by_name[std], type=pa.string()))
            else:
                arrays.append(pa.nulls(table.num_rows, type=pa.string()))
        tables.append(pa.Table.from_arrays(arrays=arrays, schema=schema))
    if not tables:
        empty = [pa.array([], type=f.type) for f in schema]
        return schema, pa.Table.from_arrays(empty, schema=schema)
    return schema, pa.concat_tables(tables, promote_options="none")
