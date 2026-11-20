"""FastAPI 入口：上传 CSV、按路段查看疑似桥接上报及其关联来源。"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import core, db

MAX_UPLOAD_BYTES = 20 * 1024 * 1024  # 20MB
TEMPLATES_DIR = Path(__file__).parent / "templates"


def create_app(db_path: str) -> FastAPI:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    db.init_db(db_path)

    app = FastAPI(title="道路巡检桥接上报复核")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    def index_context(error: str | None = None, msg: str | None = None) -> dict:
        return {
            "segments": db.list_segments(db_path),
            "uploads": db.list_uploads(db_path),
            "error": error,
            "msg": msg,
        }

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request, msg: str | None = None):
        return templates.TemplateResponse(request, "index.html", index_context(msg=msg))

    @app.post("/upload", response_class=HTMLResponse)
    async def upload(request: Request, file: UploadFile = File(...)):
        filename = file.filename or "upload.csv"
        data = await file.read()
        if len(data) > MAX_UPLOAD_BYTES:
            return templates.TemplateResponse(
                request,
                "index.html",
                index_context(error="文件超过 20MB 上限"),
                status_code=413,
            )

        # 快传只依赖原始字节的 SHA-256，不需要解码与逐行解析。判断与登记在
        # 同一事务内完成；若提交时发现来源已被并发覆盖，返回 None 且不写任何数据，
        # 再走原规则校验并整批入库。
        content_sha256 = hashlib.sha256(data).hexdigest()
        result = db.ingest_upload(
            db_path, filename, rows=None, content_sha256=content_sha256
        )
        if result is not None:
            return _redirect_success(result)

        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            return templates.TemplateResponse(
                request,
                "index.html",
                index_context(error="文件不是有效的 UTF-8 文本"),
                status_code=400,
            )
        try:
            rows = core.parse_csv(text)
        except core.CsvError as e:
            # 校验失败：整批拒绝，数据库不写入任何内容
            return templates.TemplateResponse(
                request,
                "index.html",
                index_context(error=str(e)),
                status_code=400,
            )
        result = db.ingest_upload(
            db_path,
            filename,
            rows=rows,
            content_sha256=content_sha256,
        )
        assert result is not None
        return _redirect_success(result)

    def _redirect_success(result: dict) -> RedirectResponse:
        if result["mode"] == "fast":
            msg = (
                f"快速重传：{result['row_count']} 行与批次 "
                f"{result['source_upload_id'][:8]} 逐字节相同且未被覆盖，"
                f"已跳过解析与写库（批次 {result['upload_id'][:8]}）"
            )
        else:
            msg = f"上传成功：{result['row_count']} 行已提交（批次 {result['upload_id'][:8]}）"
        return RedirectResponse(f"/?{urlencode({'msg': msg})}", status_code=303)

    @app.get("/segments/{segment_id}", response_class=HTMLResponse)
    def segment_page(request: Request, segment_id: str):
        reports = db.get_effective_reports(db_path, segment_id)
        active = [r for r in reports if not r["withdrawn"]]
        bridges = core.find_bridges(active)
        return templates.TemplateResponse(
            request,
            "segment.html",
            {
                "segment_id": segment_id,
                "reports": reports,
                "by_id": {r["report_id"]: r for r in reports},
                "bridges": bridges,
                "bridge_ids": {b["report_id"] for b in bridges},
            },
        )

    @app.get("/api/segments")
    def api_segments():
        return {"segments": db.list_segments(db_path)}

    @app.get("/api/segments/{segment_id}/bridges")
    def api_bridges(segment_id: str):
        reports = db.get_effective_reports(db_path, segment_id)
        active = [r for r in reports if not r["withdrawn"]]
        return {
            "segment_id": segment_id,
            "active_reports": sorted(r["report_id"] for r in active),
            "bridges": core.find_bridges(active),
        }

    return app


app = create_app(os.environ.get("DB_PATH", str(Path("data") / "app.db")))
