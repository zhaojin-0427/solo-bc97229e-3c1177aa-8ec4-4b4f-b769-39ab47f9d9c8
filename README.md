# 鸟类监测数据湖（Flask + PyArrow + SQLite）

把各监测站逐年新增鸟种的调查 CSV 入湖：每批一个独立 Parquet 文件，目录库（SQLite）
按列名合并出统一的逻辑结构，研究员可按站点预览 / 下载统一结构的 CSV。
在此之上提供**鸟种别名标准化**：可替换「原名 → 标准名」字典，按标准鸟种取数。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

可选：灌入示例数据（含类型提升、别名合并、标准化冲突三类演示）

```bash
docker compose exec web python scripts/seed_demo.py
docker compose exec web python scripts/demo_standardize.py
```

运行端到端自检（解析校验 / 结构合并 / 类型提升 / 并发 / 重启持久化 /
标准化规则 / 替换成功与冲突 / 不漏检 / 跨进程互斥 / 崩溃恢复 / Web 流程）：

```bash
docker compose exec web python scripts/selftest.py
```

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATA_DIR=./data
flask --app app.web run        # 或 gunicorn -b 0.0.0.0:8000 app.web:app
```

## 数据格式与入湖流程

1. **上传**：每批填写站点 + 选 CSV。CSV 首列为观测日（`YYYY-MM-DD`），其余每列一种鸟；
   列名去空白后非空且不得重复；空单元格一律按 NULL 处理。
2. **逐列声明类型**：每个鸟种列声明为 `uint64`（非负 64 位整数，0 ~ 2^64-1）或 `text`。
   首列观测日归一化为固定列名 `obs_date`（date 类型），鸟种列不得使用该保留名。
3. **发布**：
   - 解析校验（坏整数、坏日期、字段数不齐 → **整批不发布**）；
   - **按提交时生效的鸟种字典整批做标准化校验**，标准化冲突 → 整批不发布；
     若登记前字典刚好切换，则按新生效字典重新校验后重试，杜绝漏检；
   - 写 `parquet/<批次UUID>.parquet`：先写临时文件 + fsync，再原子 rename，**既有文件永不重写**；
   - 最后才开启**短暂的**目录库写事务：合并逻辑结构 + 登记批次（含发布时字典版本）。
   解析和写文件期间**不持有**目录库写事务；登记失败会删掉未发布的文件，**目录永不引用残件**。

## 鸟种别名标准化

### 字典语义

- 字典为「原名 → 标准名」的**单层**映射，只映射一次、**不递归**；未收录的原名保留原名；
- 初始 v1 为空字典（全部沿用原名），每版字典是 `dicts/dict_v<N>.json` 不可变文件；
- **每条调查（批次）独立处理**，不跨批次合值；
- 整数统一按**十进制转文本**后再比较与输出；空值一律为 NULL；
- 同一批次内映到同一标准名的多个非空值：**完全相同才合一**；全为空结果仍为 NULL；
  出现不同非空值即**冲突**，冲突位置置 NULL 并记录明细。
- **原 Parquet 与原统一结构保持不变**：标准化在读取时按生效字典即时计算，
  「原始预览 / 原始CSV」与「标准化取数」两套入口并存。

### 替换流程（后台核查，无冲突才切换）

1. 在「鸟种字典」页编辑映射（每行 `原名,标准名`，或粘贴 JSON）并提交；
2. 系统获取**跨进程排他锁**（多进程共用 `DATA_DIR` 时同时只允许一次替换），
   登记候选字典与 `scanning` 任务，后台线程**只读扫描**全部已发布观测；
   扫描不占目录库写事务，进度 / 冲突由短事务落库，页面每 2 秒刷新进度；
3. 扫描期间上传与取数**仍按旧（生效）字典**工作；期间新发布的批次会被**增量追赶扫描**，
   直到一个短事务内确认「无冲突且批次清单未增长」，才 CAS 切换生效版本；
4. 任一冲突 → 替换**失败、保留旧字典**，页面列出**冲突批次、数据行号（从 1 起）、原值**；
   异常 / 进程重启导致的未完成任务，重启时按心跳超时标记失败，生效字典始终保留；
5. 迟到的核查若发现生效版本已变化会放弃切换，不会覆盖新字典。

### 一致性取数

预览 / 下载在同一个 SQLite 读事务内取「逻辑结构 + 文件清单 + 生效字典版本」快照，
再按该快照读取不可变 Parquet 并套用同一版字典——一次取数始终使用同一状态的
结构、批次与字典。标准化下载文件名内含字典版本，例如
`birds_standard_青湾_dictv2_20261113_120000.csv`。

## 逻辑结构合并规则

- 按列名合并；批次缺列在读出时补 NULL；新鸟种自动加列；
- 同鸟种类型冲突（一边 `uint64` 一边 `text`）→ 提升为 `text`；
- 提升后**不得退回**整数（`text` 是吸收态）；旧整数读出时按**十进制**转成文本；
- 并发发布新增不同鸟种、或对同列并发提升：发布事务经 `BEGIN IMMEDIATE` 串行化，
  结构变化与各批记录都完整保留（自检第 3 项覆盖）。

## 持久化与重启

`DATA_DIR`（Docker 卷 `lake_data`）内含：

```
catalog.db      # WAL 目录库：结构、批次、字典版本、替换任务、冲突明细
parquet/        # 每批一个不可变 Parquet
staging/        # 两步表单的暂存 CSV
dicts/          # 不可变字典 JSON（dict_v1.json 起）
locks/          # 替换全程持有的 flock 文件
```

重启后直接可用；启动时清理暂存区与崩溃遗留临时文件，未被目录引用的孤儿
Parquet 过了宽限期才删除（不影响在途发布）；心跳超时的未完成替换标记为失败。

## 目录结构

```
app/
  catalog.py       # SQLite 目录库：结构、批次、字典、替换任务、冲突明细
  csvparse.py      # CSV 解析与校验（纯标准库）
  store.py         # PyArrow：批次表、原子写 Parquet、统一结构读取、CSV 导出
  standardize.py   # 字典解析、单层映射、逐批标准化与冲突检测、标准化取数
  replacement.py   # 替换编排：后台核查、增量追赶、静态点 CAS 切换
  locks.py         # 跨进程排他 flock（同时只允许一次替换）
  service.py       # 入湖编排：暂存 -> 校验(含字典) -> 写文件 -> 短事务登记；启动恢复
  web.py           # Flask 路由
  templates/       # 页面（含鸟种字典、替换进度、标准化取数页）
scripts/
  selftest.py          # 端到端自检（13 组）
  seed_demo.py         # 示例数据灌入
  demo_standardize.py  # 标准化替换成功 / 冲突演示
samples/               # 示例 CSV 与示例字典
```

## 主要 HTTP 入口

| 路由 | 说明 |
| --- | --- |
| `GET /` | 总览：逻辑结构、生效字典版本、站点、批次 |
| `GET/POST /upload` | 上传 CSV（第一步） |
| `POST /publish` | 声明类型后发布（第二步，含生效字典整批校验） |
| `GET /preview?station=&limit=` | 按站点预览**原统一结构**（不改名不合并） |
| `GET /download.csv?station=` | 下载**原统一结构** CSV |
| `GET /standard` | 鸟种字典页：版本、替换表单、最近任务与冲突、历史 |
| `POST /standard/replace` | 发起字典替换（后台核查，仅一个执行者） |
| `GET /standard/replacement/<id>` | 替换任务进度 / 终态 / 冲突明细（自动刷新） |
| `GET /standard/preview?station=&limit=` | 按生效字典预览**标准化**数据 |
| `GET /standard/download.csv?station=` | 下载**标准化** CSV（文件名含字典版本） |
| `GET /healthz` | 健康检查 |
