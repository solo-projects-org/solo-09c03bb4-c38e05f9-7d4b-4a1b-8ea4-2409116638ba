"""SQLite 数据访问层。

一致性约定：
- 每次上传 = 一个事务（BEGIN IMMEDIATE ... COMMIT）：批次记录与全部数据行
  要么一起提交，要么整体回滚，失败不留半截数据；
- 每次查询在单个只读事务内取数，桥接结论与分裂证据来自同一份已提交快照，
  不会被并行上传切成两半；
- WAL 模式 + busy_timeout：读写交错不报错；数据库文件持久化，进程重启后
  已提交数据原样保留。

修订语义：
- 同一上报号可能有多行（不同修订号），生效行 = 修订号最大的那一行；
- 高修订可以更正坐标/时刻/路段，也可以撤回（withdrawn=1）；
- 迟到的旧修订照常落库（留痕），但永远不会重新生效，无法恢复旧证据。

快速重传：
- uploads 表记录每个批次的原始文件 SHA-256（content_sha256）；
- 正常入库时，被本次写入覆盖了「上报号+修订号」的既有批次会标记
  superseded=1（同一事务内完成，失败一起回滚）；
- 若某文件与一个 superseded=0 的正常入库批次逐字节相同，则重传时只写
  uploads 一行（is_fast=1），不解析、不读写 reports 表；
- 判断与写入在同一个 BEGIN IMMEDIATE 事务里完成，因此并发时以实际提交
  顺序为准；事务失败则整体回滚，原数据与快传资格保持不变。
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS uploads (
    upload_id        TEXT PRIMARY KEY,
    filename         TEXT NOT NULL,
    uploaded_at      TEXT NOT NULL,
    row_count        INTEGER NOT NULL,
    content_sha256   TEXT,
    is_fast          INTEGER NOT NULL DEFAULT 0,
    superseded       INTEGER NOT NULL DEFAULT 0,
    source_upload_id TEXT,
    ingest_seq       INTEGER
);
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT NOT NULL,
    revision  INTEGER NOT NULL,
    segment_id TEXT NOT NULL,
    x REAL NOT NULL,
    y REAL NOT NULL,
    ts_utc TEXT NOT NULL,
    ts_epoch REAL NOT NULL,
    withdrawn INTEGER NOT NULL CHECK (withdrawn IN (0, 1)),
    upload_id TEXT NOT NULL REFERENCES uploads(upload_id),
    PRIMARY KEY (report_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_reports_segment ON reports(segment_id);
"""

# 旧库升级时逐列补齐；SQLite 的 ALTER TABLE ADD COLUMN 是事务性的。
_UPLOAD_MIGRATION_COLUMNS = (
    ("content_sha256", "TEXT"),
    ("is_fast", "INTEGER NOT NULL DEFAULT 0"),
    ("superseded", "INTEGER NOT NULL DEFAULT 0"),
    ("source_upload_id", "TEXT"),
    ("ingest_seq", "INTEGER"),
)

# 生效行 = 每个上报号修订号最大的行；路段归属也以生效行为准（更正可以改路段）
_EFFECTIVE_JOIN = """
FROM reports r
JOIN (
    SELECT report_id, MAX(revision) AS max_rev
    FROM reports
    GROUP BY report_id
) m ON m.report_id = r.report_id AND m.max_rev = r.revision
"""

# 可作为快传来源的批次：正常入库（不是快传）、之后没有任何键被覆盖、且有原始哈希。
_FAST_SOURCE_SQL = """
SELECT upload_id, row_count, ingest_seq
FROM uploads
WHERE content_sha256 = ?
  AND is_fast = 0
  AND superseded = 0
ORDER BY ingest_seq DESC
LIMIT 1
"""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _migrate_uploads(conn: sqlite3.Connection) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(uploads)")}
    for name, decl in _UPLOAD_MIGRATION_COLUMNS:
        if name not in cols:
            try:
                conn.execute(f"ALTER TABLE uploads ADD COLUMN {name} {decl}")
            except sqlite3.OperationalError as e:
                # 多个进程同时启动时可能被另一个进程抢先加列；确认列已存在即可。
                if "duplicate column name" not in str(e).lower():
                    raise
    # 旧批次没有 ingest_seq；用 rowid 回填，保证升级后仍能按入库先后排序。
    # 旧批次 content_sha256 为 NULL，因此不会成为快传来源（只从升级后正常入库开始识别）。
    conn.execute("UPDATE uploads SET ingest_seq = rowid WHERE ingest_seq IS NULL")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_uploads_ingest_seq ON uploads(ingest_seq)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_uploads_content_sha ON uploads(content_sha256)"
    )


def init_db(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _migrate_uploads(conn)
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()


def _new_upload_id() -> str:
    return uuid.uuid4().hex


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _next_ingest_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(ingest_seq), 0) + 1 FROM uploads").fetchone()
    return int(row[0])


def _insert_upload_row(
    conn: sqlite3.Connection,
    filename: str,
    row_count: int,
    content_sha256: str | None,
    *,
    is_fast: bool,
    source_upload_id: str | None,
) -> dict:
    upload_id = _new_upload_id()
    uploaded_at = _now_iso()
    conn.execute(
        """INSERT INTO uploads
           (upload_id, filename, uploaded_at, row_count, content_sha256,
            is_fast, superseded, source_upload_id, ingest_seq)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            upload_id,
            filename,
            uploaded_at,
            row_count,
            content_sha256,
            1 if is_fast else 0,
            0,
            source_upload_id,
            _next_ingest_seq(conn),
        ),
    )
    return {
        "upload_id": upload_id,
        "filename": filename,
        "uploaded_at": uploaded_at,
        "row_count": row_count,
        "is_fast": is_fast,
        "source_upload_id": source_upload_id,
        "mode": "fast" if is_fast else "normal",
    }


def _mark_overwritten_sources(conn: sqlite3.Connection, rows) -> None:
    """把本次将要覆盖的 (report_id, revision) 的现有归属批次标为 superseded。

    必须在写入新 reports 行之前执行，且与写入处于同一事务。
    """
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS incoming_keys ("
        "  report_id TEXT NOT NULL,"
        "  revision INTEGER NOT NULL,"
        "  PRIMARY KEY (report_id, revision)"
        ")"
    )
    conn.execute("DELETE FROM incoming_keys")
    conn.executemany(
        "INSERT OR IGNORE INTO incoming_keys (report_id, revision) VALUES (?, ?)",
        [(r.report_id, r.revision) for r in rows],
    )
    conn.execute(
        """UPDATE uploads SET superseded = 1
           WHERE superseded = 0
             AND upload_id IN (
                 SELECT r.upload_id
                 FROM reports r
                 JOIN incoming_keys k
                   ON k.report_id = r.report_id AND k.revision = r.revision
             )"""
    )
    conn.execute("DELETE FROM incoming_keys")


def find_fast_source(db_path: str, content_sha256: str) -> dict | None:
    """只读查询（自测/诊断用）：是否存在可快传的来源批次。

    上传入口不再先做这一步，而是在 ingest_upload 的事务内判断，避免多一次
    连接与 TOCTOU 窗口。
    """
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        row = conn.execute(_FAST_SOURCE_SQL, (content_sha256,)).fetchone()
        conn.execute("COMMIT")
        return dict(row) if row is not None else None
    finally:
        conn.close()


def ingest_upload(
    db_path: str,
    filename: str,
    rows=None,
    content_sha256: str | None = None,
) -> dict | None:
    """提交一个批次。

    - 若 content_sha256 命中一个仍有效的正常入库批次：只登记 uploads 行
      （is_fast=1），返回 {"mode": "fast", ...}；
    - 否则需要 rows，按原规则整批写入，返回 {"mode": "normal", ...}；
    - 传入 rows=None 但提交时未命中可快传来源时返回 None（不写任何数据），
      调用方应随后解析并按原规则整批入库。

    快传判断与写入都在同一个 BEGIN IMMEDIATE 事务内，因此并发时以实际
    提交顺序判定；任何异常都会 ROLLBACK，原数据和快传资格不受影响。
    """
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if content_sha256 is not None:
            source = conn.execute(_FAST_SOURCE_SQL, (content_sha256,)).fetchone()
            if source is not None:
                result = _insert_upload_row(
                    conn,
                    filename,
                    int(source["row_count"]),
                    content_sha256,
                    is_fast=True,
                    source_upload_id=source["upload_id"],
                )
                conn.execute("COMMIT")
                return result
            if rows is None:
                # 提交时未命中来源（并发覆盖可能刚提交）：不写任何东西，
                # 交还调用方按原规则解析并整批入库。
                conn.execute("ROLLBACK")
                return None

        if rows is None:
            raise ValueError("正常入库需要已解析的 rows")

        upload_id = _new_upload_id()
        # 先标记将被覆盖的旧批次，再写新行；两步同一事务。
        _mark_overwritten_sources(conn, rows)
        uploaded_at = _now_iso()
        conn.execute(
            """INSERT INTO uploads
               (upload_id, filename, uploaded_at, row_count, content_sha256,
                is_fast, superseded, source_upload_id, ingest_seq)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                upload_id,
                filename,
                uploaded_at,
                len(rows),
                content_sha256,
                0,
                0,
                None,
                _next_ingest_seq(conn),
            ),
        )
        conn.executemany(
            """INSERT OR REPLACE INTO reports
               (report_id, revision, segment_id, x, y, ts_utc, ts_epoch, withdrawn, upload_id)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            [
                (
                    r.report_id,
                    r.revision,
                    r.segment_id,
                    r.x,
                    r.y,
                    r.ts_utc,
                    r.ts_epoch,
                    1 if r.withdrawn else 0,
                    upload_id,
                )
                for r in rows
            ],
        )
        conn.execute("COMMIT")
        return {
            "upload_id": upload_id,
            "filename": filename,
            "uploaded_at": uploaded_at,
            "row_count": len(rows),
            "is_fast": False,
            "source_upload_id": None,
            "mode": "normal",
        }
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def save_upload(db_path: str, filename: str, rows) -> str:
    """兼容旧调用：正常整批入库，返回批次号。"""
    result = ingest_upload(db_path, filename, rows)
    assert result is not None
    return result["upload_id"]


def list_uploads(db_path: str) -> list[dict]:
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT upload_id, filename, uploaded_at, row_count, content_sha256, "
            "       is_fast, source_upload_id "
            "FROM uploads ORDER BY ingest_seq DESC, upload_id DESC"
        ).fetchall()
        conn.execute("COMMIT")
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_segments(db_path: str) -> list[dict]:
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT r.segment_id AS segment_id, "
            "       COUNT(*) AS n_effective, "
            "       SUM(CASE WHEN r.withdrawn = 0 THEN 1 ELSE 0 END) AS n_active "
            + _EFFECTIVE_JOIN
            + " GROUP BY r.segment_id ORDER BY r.segment_id"
        ).fetchall()
        conn.execute("COMMIT")
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_effective_reports(db_path: str, segment_id: str) -> list[dict]:
    """取某路段当前生效（最高修订号）的全部上报，含已撤回行（页面标注展示）。
    单个只读事务内完成，保证后续桥接计算基于同一份已提交快照。"""
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT r.* " + _EFFECTIVE_JOIN + " WHERE r.segment_id = ? ORDER BY r.report_id",
            (segment_id,),
        ).fetchall()
        conn.execute("COMMIT")
        return [dict(r) for r in rows]
    finally:
        conn.close()
