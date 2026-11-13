"""Flask Web 入口。

原有：上传批次（两步：上传 -> 逐列声明类型）、按站点预览、下载统一结构 CSV。
新增：鸟种字典管理与替换（/standard）、按生效字典标准化预览 / 下载。
"""
import datetime
import io
import os

from flask import (Flask, flash, redirect, render_template, request,
                   send_file, url_for)

from . import catalog, csvparse, replacement, service, standardize, store
from .config import Config
from .locks import LockBusy

MAX_UPLOAD_BYTES = 64 * 1024 * 1024


def create_app(config=None):
    cfg = config or Config()
    service.init_storage(cfg)

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
    app.secret_key = os.environ.get("SECRET_KEY", "bird-lake-dev-secret")

    def snapshot(station=None):
        conn = catalog.connect(cfg.db_path)
        try:
            return catalog.snapshot(conn, station)
        finally:
            conn.close()

    def load_active_mapping(snap):
        """按快照中的生效字典版本读取不可变字典文件（同一次取数同一状态）。"""
        conn = catalog.connect(cfg.db_path)
        try:
            row = catalog.get_dictionary(conn, snap["active_dict_version"])
        finally:
            conn.close()
        if row is None:  # 极端情况：字典记录缺失，退化为空字典（沿用原名）
            return {}
        return standardize.load_mapping(os.path.join(cfg.dict_dir, row["dict_file"]))

    @app.get("/")
    def index():
        snap = snapshot()
        stations = {}
        for b in snap["batches"]:
            s = stations.setdefault(
                b["station"], {"station": b["station"], "n_batches": 0, "n_rows": 0})
            s["n_batches"] += 1
            s["n_rows"] += b["n_rows"]
        recent = list(reversed(snap["batches"]))[:50]
        return render_template("index.html", snap=snap,
                               stations=sorted(stations.values(), key=lambda x: x["station"]),
                               recent=recent)

    @app.get("/upload")
    def upload_form():
        return render_template("upload.html")

    @app.post("/upload")
    def upload():
        station = (request.form.get("station") or "").strip()
        f = request.files.get("file")
        if not station:
            flash("请填写站点", "err")
            return redirect(url_for("upload_form"))
        if f is None or not f.filename:
            flash("请选择 CSV 文件", "err")
            return redirect(url_for("upload_form"))
        token = service.stage_upload(cfg, f.stream)
        try:
            header = service.staged_header(cfg, token)
        except csvparse.CsvError as e:
            service.discard_staged(cfg, token)
            flash(f"CSV 表头无效：{e}", "err")
            return redirect(url_for("upload_form"))
        if store.DATE_COLUMN in header[1:]:
            service.discard_staged(cfg, token)
            flash(f"鸟种列不能使用保留列名 {store.DATE_COLUMN}", "err")
            return redirect(url_for("upload_form"))
        return render_template("declare.html", token=token, station=station,
                               filename=f.filename, date_col=header[0],
                               columns=header[1:])

    @app.post("/publish")
    def publish():
        token = request.form.get("token") or ""
        station = (request.form.get("station") or "").strip()
        filename = request.form.get("filename") or ""
        try:
            path = service.staged_path(cfg, token)
        except ValueError as e:
            flash(str(e), "err")
            return redirect(url_for("upload_form"))
        try:
            header = service.staged_header(cfg, token)
            types = [request.form.get(f"type_{i}", "") for i in range(len(header) - 1)]
            result = service.publish(cfg, station, filename, path, types)
        except (csvparse.CsvError, ValueError) as e:
            flash(f"批次未发布：{e}", "err")
            return redirect(url_for("upload_form"))
        except OSError as e:
            flash(f"批次未发布：写入存储失败（{e}）", "err")
            return redirect(url_for("upload_form"))
        finally:
            service.discard_staged(cfg, token)
        parts = [f"批次 {result['batch_id'][:8]} 已发布：站点「{result['station']}」，"
                 f"{result['n_rows']} 行（生效字典 v{result['dict_version']}）"]
        for c in result["changes"]:
            if c["change"] == "added":
                parts.append(f"新增列 {c['name']}({c['type']})")
            elif c["change"] == "promoted":
                parts.append(f"列 {c['name']} 类型提升为 text")
        flash("；".join(parts), "ok")
        return redirect(url_for("preview", station=result["station"]))

    # ---------- 原统一结构（保留，不改名不合并） ----------

    @app.get("/preview")
    def preview():
        station = request.args.get("station") or None
        try:
            limit = max(1, min(int(request.args.get("limit", "200")), 5000))
        except ValueError:
            limit = 200
        # 同一次取数：一个读快照内的逻辑结构 + 文件清单 + 生效字典版本
        snap = snapshot(station)
        schema, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
        rows = table.slice(0, limit).to_pylist()
        all_stations = sorted({b["station"] for b in snapshot()["batches"]})
        return render_template("preview.html", station=station, stations=all_stations,
                               columns=schema.names, rows=rows, total=table.num_rows,
                               limit=limit, version=snap["schema_version"],
                               dict_version=snap["active_dict_version"])

    @app.get("/download.csv")
    def download_csv():
        station = request.args.get("station") or None
        snap = snapshot(station)
        _schema, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
        data = store.table_to_csv_bytes(table)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"birds_{station or 'all'}_{ts}.csv"
        return send_file(io.BytesIO(data), mimetype="text/csv",
                         as_attachment=True, download_name=name)

    # ---------- 鸟种字典与标准化取数 ----------

    @app.get("/standard")
    def standard():
        snap = snapshot()
        conn = catalog.connect(cfg.db_path)
        try:
            dicts = catalog.list_dictionaries(conn)
            history = catalog.list_replacements(conn)
        finally:
            conn.close()
        latest = replacement.get_status(cfg)
        # 生效字典的当前映射，预填到替换表单
        active_map = load_active_mapping(snap)
        active_lines = "\n".join(f"{k},{v}" for k, v in sorted(active_map.items()))
        return render_template("standard.html", snap=snap, dicts=dicts,
                               history=history, latest=latest,
                               active_lines=active_lines)

    @app.post("/standard/replace")
    def standard_replace():
        text = request.form.get("mapping") or ""
        try:
            mapping = standardize.parse_mapping_text(text)
        except standardize.DictError as e:
            flash(f"字典无效：{e}", "err")
            return redirect(url_for("standard"))
        try:
            repl_id = replacement.request_start(cfg, mapping)
        except LockBusy as e:
            flash(str(e), "err")
            return redirect(url_for("standard"))
        except RuntimeError as e:
            flash(str(e), "err")
            return redirect(url_for("standard"))
        flash(f"替换任务 #{repl_id} 已启动：后台核查全部已发布观测，期间上传与取数仍按旧字典", "ok")
        return redirect(url_for("replacement_status", repl_id=repl_id))

    @app.get("/standard/replacement/<int:repl_id>")
    def replacement_status(repl_id):
        status = replacement.get_status(cfg, repl_id)
        if status is None:
            flash("替换任务不存在", "err")
            return redirect(url_for("standard"))
        return render_template("replacement.html", status=status)

    @app.get("/standard/preview")
    def standard_preview():
        station = request.args.get("station") or None
        try:
            limit = max(1, min(int(request.args.get("limit", "200")), 5000))
        except ValueError:
            limit = 200
        # 同一次取数：一个读快照内的逻辑结构 + 文件清单 + 生效字典版本
        snap = snapshot(station)
        mapping = load_active_mapping(snap)
        try:
            schema, table = standardize.load_standardized(
                cfg.parquet_dir, snap["columns"], snap["batches"], mapping)
        except standardize.DictError as e:
            flash(str(e), "err")
            return redirect(url_for("standard"))
        rows = table.slice(0, limit).to_pylist()
        all_stations = sorted({b["station"] for b in snapshot()["batches"]})
        return render_template("standard_preview.html", station=station,
                               stations=all_stations, columns=schema.names, rows=rows,
                               total=table.num_rows, limit=limit,
                               dict_version=snap["active_dict_version"])

    @app.get("/standard/download.csv")
    def standard_download():
        station = request.args.get("station") or None
        snap = snapshot(station)
        mapping = load_active_mapping(snap)
        try:
            _schema, table = standardize.load_standardized(
                cfg.parquet_dir, snap["columns"], snap["batches"], mapping)
        except standardize.DictError as e:
            flash(str(e), "err")
            return redirect(url_for("standard"))
        data = store.table_to_csv_bytes(table)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"birds_standard_{station or 'all'}_dictv{snap['active_dict_version']}_{ts}.csv"
        return send_file(io.BytesIO(data), mimetype="text/csv",
                         as_attachment=True, download_name=name)

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    @app.errorhandler(413)
    def too_large(_e):
        flash("文件过大（上限 64MB）", "err")
        return redirect(url_for("upload_form"))

    return app


app = create_app()
