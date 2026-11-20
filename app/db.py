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
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS uploads (
    upload_id   TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    uploaded_at TEXT NOT NULL,
    row_count   INTEGER NOT NULL
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

# 生效行 = 每个上报号修订号最大的行；路段归属也以生效行为准（更正可以改路段）
_EFFECTIVE_JOIN = """
FROM reports r
JOIN (
    SELECT report_id, MAX(revision) AS max_rev
    FROM reports
    GROUP BY report_id
) m ON m.report_id = r.report_id AND m.max_rev = r.revision
"""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def save_upload(db_path: str, filename: str, rows) -> str:
    """把一个已校验的 CSV 批次整体提交。任何失败都会回滚，不留部分数据。"""
    upload_id = uuid.uuid4().hex
    uploaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO uploads (upload_id, filename, uploaded_at, row_count) VALUES (?,?,?,?)",
            (upload_id, filename, uploaded_at, len(rows)),
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
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()
    return upload_id


def list_uploads(db_path: str) -> list[dict]:
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT upload_id, filename, uploaded_at, row_count "
            "FROM uploads ORDER BY uploaded_at DESC, upload_id DESC"
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
