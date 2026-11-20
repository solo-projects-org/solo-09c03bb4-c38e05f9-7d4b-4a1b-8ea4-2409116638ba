"""关键自测：桥接判定、修订语义、上传原子性、重启持久化、交错一致性、快速重传。"""
import hashlib
import sqlite3
import threading
from urllib.parse import unquote

import pytest
from fastapi.testclient import TestClient

from app import core, db
from app.main import create_app

HEADER = "report_id,revision,segment_id,x,y,ts_utc,withdrawn"

# 经典链：A—B—C（B 是桥接）
CHAIN = [
    ("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
    ("B", 0, "S1", 4, 0, "2026-11-19T10:05:00Z", 0),
    ("C", 0, "S1", 8, 0, "2026-11-19T10:10:00Z", 0),
]


def make_csv(rows, header=HEADER):
    lines = [header]
    for r in rows:
        lines.append(",".join(str(c) for c in r))
    return "\n".join(lines) + "\n"


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def post(client, text, filename="data.csv"):
    return client.post(
        "/upload", files={"file": (filename, text.encode("utf-8"), "text/csv")}
    )


def get_bridges(client, segment="S1"):
    r = client.get(f"/api/segments/{segment}/bridges")
    assert r.status_code == 200
    return r.json()


# ---------------------------------------------------------------- 桥接判定


def test_chain_middle_is_bridge(client):
    r = post(client, make_csv(CHAIN))
    assert r.status_code == 200  # 303 重定向后跟到首页
    data = get_bridges(client)
    assert data["active_reports"] == ["A", "B", "C"]
    assert data["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]


def test_triangle_has_no_bridge(client):
    rows = [
        ("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S1", 4, 0, "2026-11-19T10:00:00Z", 0),
        ("C", 0, "S1", 2, 3, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert get_bridges(client)["bridges"] == []


def test_pair_edge_points_are_not_bridges(client):
    """两条互连上报：移除任一条只剩一个非空组，不算桥接。"""
    rows = [
        ("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S1", 5, 0, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert get_bridges(client)["bridges"] == []


def test_star_center_splits_into_three_groups(client):
    rows = [
        ("B", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("A", 0, "S1", -4, 0, "2026-11-19T10:00:00Z", 0),
        ("C", 0, "S1", 4, 0, "2026-11-19T10:00:00Z", 0),
        ("D", 0, "S1", 0, 4, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert get_bridges(client)["bridges"] == [
        {"report_id": "B", "groups": [["A"], ["C"], ["D"]]}
    ]


def test_distance_threshold_inclusive(client):
    """恰好 5 米相连，5.01 米不相连。"""
    rows = [
        ("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S1", 5, 0, "2026-11-19T10:00:00Z", 0),
        ("C", 0, "S1", 10, 0, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert [b["report_id"] for b in get_bridges(client)["bridges"]] == ["B"]

    rows2 = [
        ("A", 0, "S2", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S2", 5.01, 0, "2026-11-19T10:00:00Z", 0),
        ("C", 0, "S2", 10.02, 0, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows2))
    assert get_bridges(client, "S2")["bridges"] == []


def test_time_threshold_inclusive(client):
    """恰好 10 分钟相连，10 分 01 秒不相连。"""
    rows = [
        ("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S1", 4, 0, "2026-11-19T10:10:00Z", 0),
        ("C", 0, "S1", 8, 0, "2026-11-19T10:20:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert [b["report_id"] for b in get_bridges(client)["bridges"]] == ["B"]

    rows2 = [
        ("A", 0, "S2", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S2", 4, 0, "2026-11-19T10:10:01Z", 0),
        ("C", 0, "S2", 8, 0, "2026-11-19T10:20:02Z", 0),
    ]
    post(client, make_csv(rows2))
    assert get_bridges(client, "S2")["bridges"] == []


def test_cross_segment_not_connected(client):
    """不同路段即使距离/时刻满足也不相连。"""
    rows = [
        ("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0),
        ("B", 0, "S1", 4, 0, "2026-11-19T10:05:00Z", 0),
        ("C", 0, "S2", 8, 0, "2026-11-19T10:10:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert get_bridges(client, "S1")["bridges"] == []
    assert get_bridges(client, "S2")["bridges"] == []


def test_split_groups_only_within_own_component(client):
    """分裂组只统计原连通组：链 A-B-C 与远处一对 D-E 同路段，
    B 的分裂组不含 D-E；D/E 是叶子，不是桥接。"""
    rows = CHAIN + [
        ("D", 0, "S1", 100, 0, "2026-11-19T10:00:00Z", 0),
        ("E", 0, "S1", 104, 0, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert get_bridges(client)["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]


def test_chain_and_star_in_one_segment(client):
    """链 + 星形两个连通组：B、D 各自为桥接，分裂组互不串扰。"""
    rows = CHAIN + [
        ("D", 0, "S1", 100, 0, "2026-11-19T10:00:00Z", 0),
        ("E", 0, "S1", 104, 0, "2026-11-19T10:00:00Z", 0),
        ("F", 0, "S1", 96, 0, "2026-11-19T10:00:00Z", 0),
        ("G", 0, "S1", 100, 4, "2026-11-19T10:00:00Z", 0),
    ]
    post(client, make_csv(rows))
    assert get_bridges(client)["bridges"] == [
        {"report_id": "B", "groups": [["A"], ["C"]]},
        {"report_id": "D", "groups": [["E"], ["F"], ["G"]]},
    ]


# ---------------------------------------------------------------- 修订语义


def test_higher_revision_corrects_position(client):
    post(client, make_csv(CHAIN))
    assert [b["report_id"] for b in get_bridges(client)["bridges"]] == ["B"]
    # B 的高修订把它挪远：链断开，不再有桥接
    post(client, make_csv([("B", 1, "S1", 100, 0, "2026-11-19T10:05:00Z", 0)]))
    data = get_bridges(client)
    assert data["bridges"] == []
    assert data["active_reports"] == ["A", "B", "C"]


def test_higher_revision_withdraws(client):
    post(client, make_csv(CHAIN))
    post(client, make_csv([("B", 1, "S1", 4, 0, "2026-11-19T10:05:00Z", 1)]))
    data = get_bridges(client)
    assert data["active_reports"] == ["A", "C"]  # 已撤回不参与连通
    assert data["bridges"] == []


def test_late_old_revision_cannot_restore(client):
    """迟到旧修订不能恢复旧证据：先生效 rev5，再传 rev2 无效。"""
    rows = [r if r[0] != "B" else ("B", 5, "S1", 4, 0, "2026-11-19T10:05:00Z", 0) for r in CHAIN]
    post(client, make_csv(rows))
    # 迟到的旧修订 rev2：位置挪走且标记撤回 —— 必须被忽略
    post(client, make_csv([("B", 2, "S1", 100, 0, "2026-11-19T10:05:00Z", 1)]))
    data = get_bridges(client)
    assert data["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]
    # 页面上 B 的生效修订号仍是 5
    html = client.get("/segments/S1").text
    assert ">5</td>" in html


def test_revision_can_move_segment(client):
    """高修订把上报改到别的路段：原路段不再计入。"""
    post(client, make_csv(CHAIN))
    post(client, make_csv([("C", 1, "S2", 8, 0, "2026-11-19T10:10:00Z", 0)]))
    assert get_bridges(client, "S1")["active_reports"] == ["A", "B"]
    assert get_bridges(client, "S2")["active_reports"] == ["C"]


# ---------------------------------------------------------------- 原子性与一致性


def test_failed_upload_commits_nothing(client):
    """一行有错，整批拒绝：已解析成功的行也不入库。"""
    bad = make_csv(
        [("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 0), ("B", 0, "S1", "abc", 0, "2026-11-19T10:05:00Z", 0)]
    )
    r = post(client, bad)
    assert r.status_code == 400
    assert client.get("/api/segments").json()["segments"] == []
    assert client.get("/").status_code == 200


def test_interleaved_uploads_and_queries(client):
    """上传与查询交错：每次查询结果都精确对应当前已提交的批次集合。"""
    post(client, make_csv(CHAIN[:2]))  # A,B
    assert get_bridges(client)["bridges"] == []  # 仅一对，无桥接

    post(client, make_csv([CHAIN[2]]))  # +C
    assert get_bridges(client)["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]

    post(client, make_csv([("X", 0, "S1", "bad", 0, "2026-11-19T10:00:00Z", 0)]))  # 失败批次
    assert get_bridges(client)["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]

    post(client, make_csv([("B", 1, "S1", 100, 0, "2026-11-19T10:05:00Z", 0)]))  # B 挪远
    assert get_bridges(client)["bridges"] == []


def test_restart_keeps_committed_state(tmp_path):
    """重启（重新创建 app，同一数据库文件）后结论一致。"""
    db_path = str(tmp_path / "restart.db")
    with TestClient(create_app(db_path)) as c1:
        post(c1, make_csv(CHAIN))
        before = get_bridges(c1)["bridges"]
    with TestClient(create_app(db_path)) as c2:
        assert get_bridges(c2)["bridges"] == before
        assert c2.get("/segments/S1").status_code == 200


def test_reupload_same_file_is_idempotent(client):
    post(client, make_csv(CHAIN))
    post(client, make_csv(CHAIN))
    segs = client.get("/api/segments").json()["segments"]
    assert segs == [{"segment_id": "S1", "n_effective": 3, "n_active": 3}]
    assert get_bridges(client)["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]


def test_concurrent_writers_and_readers(tmp_path):
    """多线程上传/读取交错：不报错，且最终已提交集合完整。"""
    db_path = str(tmp_path / "conc.db")
    db.init_db(db_path)
    errors = []

    def writer(n):
        try:
            for i in range(10):
                row = core.ReportRow(
                    f"W{n}-{i}", 0, "S1", float(i * 10 + n), 0.0,
                    "2026-11-19T10:00:00Z", 0.0, False,
                )
                db.save_upload(db_path, f"w{n}.csv", [row])
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    def reader():
        try:
            for _ in range(50):
                reps = db.get_effective_reports(db_path, "S1")
                core.find_bridges(reps)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(3)]
    threads += [threading.Thread(target=reader) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(db.get_effective_reports(db_path, "S1")) == 30
    assert len(db.list_uploads(db_path)) == 30


# ---------------------------------------------------------------- 页面与解析


def test_segment_page_lists_groups(client):
    post(client, make_csv(CHAIN))
    html = client.get("/segments/S1").text
    assert "疑似桥接上报（1 条）" in html
    assert "⭐ 上报号 B" in html
    assert "分裂为 2 个非空组" in html


def test_parse_chinese_headers_and_naive_ts():
    text = "上报号,修订号,路段号,x,y,UTC时刻,撤回标记\nA,0,S1,1.5,2.5,2026-11-19 10:00:00,\n"
    rows = core.parse_csv(text)
    assert len(rows) == 1
    assert rows[0].report_id == "A"
    assert rows[0].withdrawn is False
    assert rows[0].ts_utc.endswith("+00:00")  # 无时区按 UTC


def test_parse_rejects_bad_input():
    with pytest.raises(core.CsvError):
        core.parse_csv("")  # 空文件
    with pytest.raises(core.CsvError):
        core.parse_csv("report_id,revision\nA,0\n")  # 缺列
    with pytest.raises(core.CsvError):
        core.parse_csv(make_csv([("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", "maybe")]))
    with pytest.raises(core.CsvError):  # 同文件内重复 上报号+修订号
        core.parse_csv(make_csv([CHAIN[0], CHAIN[0]]))


# ---------------------------------------------------------------- 快速重传


def upload_history(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT upload_id, filename, row_count, content_sha256, is_fast, "
                "superseded, source_upload_id FROM uploads ORDER BY ingest_seq"
            )
        ]
    finally:
        conn.close()


def report_owner(db_path, report_id, revision):
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT upload_id, x, withdrawn FROM reports "
            "WHERE report_id=? AND revision=?",
            (report_id, revision),
        ).fetchone()
        return row
    finally:
        conn.close()


def test_identical_reupload_is_fast(client, tmp_path):
    text = make_csv(CHAIN)
    post(client, text, filename="cracks.csv")
    history = upload_history(str(tmp_path / "test.db"))
    assert len(history) == 1 and history[0]["is_fast"] == 0

    r = post(client, text, filename="cracks-again.csv")
    assert r.status_code == 200
    assert "快速重传" in r.text
    history = upload_history(str(tmp_path / "test.db"))
    assert len(history) == 2
    fast = history[1]
    assert fast["is_fast"] == 1
    assert fast["filename"] == "cracks-again.csv"
    assert fast["row_count"] == 3  # 原文件行数
    assert fast["source_upload_id"] == history[0]["upload_id"]
    # 数据未被重写：reports 归属仍是首批次
    assert report_owner(str(tmp_path / "test.db"), "A", 0)[0] == history[0]["upload_id"]


def test_fast_reupload_skips_csv_parsing(client, monkeypatch):
    """快传路径不调用 parse_csv（证明未逐行解析）也不触碰 reports 表。"""
    text = make_csv(CHAIN)
    post(client, text)

    def boom(_text):
        raise AssertionError("快传不应解析 CSV")

    monkeypatch.setattr(core, "parse_csv", boom)
    r = post(client, text, filename="retry.csv")
    assert "快速重传" in r.text
    assert get_bridges(client)["bridges"] == [
        {"report_id": "B", "groups": [["A"], ["C"]]}
    ]


def test_fast_reupload_does_not_touch_reports_table(client, monkeypatch):
    """快传路径的 SQL 完全不涉及 reports（修订表）。"""
    text = make_csv(CHAIN)
    post(client, text)

    statements = []
    original_connect = db._connect

    def traced_connect(path):
        conn = original_connect(path)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(db, "_connect", traced_connect)
    r = client.post(
        "/upload",
        files={"file": ("retry.csv", text.encode("utf-8"), "text/csv")},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "快速重传" in unquote(r.headers["location"])
    assert statements, "应至少执行 uploads 相关语句"
    assert not any("reports" in s.lower() for s in statements)


def test_fast_reupload_listed_in_history_page(client):
    text = make_csv(CHAIN)
    post(client, text)
    post(client, text)
    html = client.get("/").text
    assert "快速重传" in html
    assert "正常入库" in html


def test_fast_reupload_survives_restart(tmp_path):
    db_file = str(tmp_path / "restart-fast.db")
    text = make_csv(CHAIN)
    with TestClient(create_app(db_file)) as c1:
        post(c1, text)
    with TestClient(create_app(db_file)) as c2:
        r = post(c2, text)
        assert "快速重传" in r.text
    history = upload_history(db_file)
    assert [h["is_fast"] for h in history] == [0, 1]


def test_other_report_does_not_break_fast(client, tmp_path):
    text = make_csv(CHAIN)
    post(client, text)
    post(client, make_csv([("Z", 0, "S1", 500, 500, "2026-11-19T11:00:00Z", 0)]))
    r = post(client, text)
    assert "快速重传" in r.text
    history = upload_history(str(tmp_path / "test.db"))
    assert history[-1]["is_fast"] == 1


def test_other_revision_of_same_report_does_not_break_fast(client, tmp_path):
    text = make_csv(CHAIN)
    post(client, text)
    post(client, make_csv([("B", 1, "S1", 4, 0, "2026-11-19T10:05:00Z", 0)]))
    r = post(client, text)
    assert "快速重传" in r.text
    assert upload_history(str(tmp_path / "test.db"))[-1]["is_fast"] == 1


def test_overwriting_same_key_disables_fast_and_reingests(client, tmp_path):
    text = make_csv(CHAIN)
    post(client, text)  # A,B,C
    # 同号同修订覆盖 B#0：内容改为撤回且挪远
    post(client, make_csv([("B", 0, "S1", 100, 100, "2026-11-19T10:05:00Z", 1)]))
    r = post(client, text)
    assert "快速重传" not in r.text
    assert "上传成功" in r.text
    history = upload_history(str(tmp_path / "test.db"))
    assert [h["is_fast"] for h in history] == [0, 0, 0]
    # 整批按原文件内容重新入库：B#0 恢复为未撤回且坐标回到文件值
    owner, x, withdrawn = report_owner(str(tmp_path / "test.db"), "B", 0)
    assert x == 4 and withdrawn == 0
    assert owner == history[-1]["upload_id"]
    assert get_bridges(client)["bridges"] == [{"report_id": "B", "groups": [["A"], ["C"]]}]


def test_partial_cover_reingests_whole_file(client, tmp_path):
    """文件中任一键被覆盖即整批入库，未覆盖的键也归入新批次。"""
    text = make_csv(CHAIN)
    post(client, text)
    post(client, make_csv([("C", 0, "S1", 50, 0, "2026-11-19T10:10:00Z", 0)]))
    r = post(client, text)
    assert "快速重传" not in r.text
    history = upload_history(str(tmp_path / "test.db"))
    newest = history[-1]["upload_id"]
    for rid in ("A", "B", "C"):
        assert report_owner(str(tmp_path / "test.db"), rid, 0)[0] == newest


def test_fast_eligibility_regained_after_normal_reingest(client, tmp_path):
    text = make_csv(CHAIN)
    post(client, text)
    post(client, make_csv([("A", 0, "S1", 0, 0, "2026-11-19T10:00:00Z", 1)]))  # 覆盖 A#0
    post(client, text)  # 正常重入，恢复
    r = post(client, text)  # 再次重传 → 快传
    assert "快速重传" in r.text


def test_race_fallback_writes_nothing(tmp_path):
    """提交时来源已被并发覆盖：不写任何数据，交回调用方重新解析入库。"""
    db_file = str(tmp_path / "race.db")
    db.init_db(db_file)
    rows = core.parse_csv(make_csv(CHAIN))
    text = make_csv(CHAIN)
    sha = hashlib.sha256(text.encode()).hexdigest()
    first = db.ingest_upload(db_file, "a.csv", rows, sha)
    assert first["mode"] == "normal"
    assert db.find_fast_source(db_file, sha) is not None

    # 模拟并发：另一个修正先提交，覆盖 B#0
    db.ingest_upload(
        db_file,
        "fix.csv",
        core.parse_csv(make_csv([("B", 0, "S1", 9, 0, "2026-11-19T10:05:00Z", 0)])),
    )
    before = db.list_uploads(db_file)
    assert db.find_fast_source(db_file, sha) is None  # 预判已失效
    assert db.ingest_upload(db_file, "a-retry.csv", None, sha) is None
    assert db.list_uploads(db_file) == before
    # 数据未被快传路径改动
    assert report_owner(db_file, "B", 0)[1] == 9

    # 调用方重新解析后整批入库
    result = db.ingest_upload(db_file, "a-retry.csv", rows, sha)
    assert result is not None and result["mode"] == "normal"
    assert report_owner(db_file, "B", 0)[1] == 4


def test_failed_ingest_keeps_data_and_fast_eligibility(tmp_path):
    """写入失败整体回滚：原数据、覆盖标记与快传资格都不变。"""
    db_file = str(tmp_path / "fail.db")
    db.init_db(db_file)
    text = make_csv(CHAIN)
    sha = hashlib.sha256(text.encode()).hexdigest()
    rows = core.parse_csv(text)
    db.ingest_upload(db_file, "a.csv", rows, sha)

    # A 的坐标不可为 NULL：覆盖 A#0 的批次会在写入阶段触发 IntegrityError，
    # 此时 superseded 标记已在本事务内执行，必须随事务一起回滚。
    bad_rows = list(rows)
    bad_rows[0] = core.ReportRow(
        "A", 0, "S1", None, 0, "2026-11-19T10:00:00+00:00", 0.0, False
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.ingest_upload(db_file, "bad.csv", bad_rows, "deadbeef")

    history = upload_history(db_file)
    assert len(history) == 1
    assert history[0]["superseded"] == 0
    assert db.find_fast_source(db_file, sha) is not None
    assert report_owner(db_file, "A", 0)[1] == 0


def test_concurrent_identical_uploads_one_normal_rest_fast(tmp_path):
    """并发上传同一文件：恰好一个正常入库，其余快传，行数不重复。"""
    db_file = str(tmp_path / "same.db")
    db.init_db(db_file)
    text = make_csv(CHAIN)
    sha = hashlib.sha256(text.encode()).hexdigest()
    rows = core.parse_csv(text)
    barrier = threading.Barrier(6)
    results = []
    errors = []

    def worker(n):
        try:
            barrier.wait()
            # 每个线程都独立预判（可能都看到“无快传来源”）
            source = db.find_fast_source(db_file, sha)
            result = db.ingest_upload(
                db_file, f"w{n}.csv", None if source else rows, sha
            )
            results.append(result["mode"] if result else "fallback")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    history = upload_history(db_file)
    assert len(history) == 6
    assert sum(1 for h in history if h["is_fast"] == 0) == 1
    assert sum(1 for h in history if h["is_fast"] == 1) == 5
    assert len(db.get_effective_reports(db_file, "S1")) == 3


def test_upgrade_old_schema_starts_fast_tracking(tmp_path):
    """旧库升级：老批次不作为快传来源；升级后正常入库的文件可快传。"""
    db_file = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE uploads (
            upload_id TEXT PRIMARY KEY, filename TEXT NOT NULL,
            uploaded_at TEXT NOT NULL, row_count INTEGER NOT NULL);
        CREATE TABLE reports (
            report_id TEXT NOT NULL, revision INTEGER NOT NULL,
            segment_id TEXT NOT NULL, x REAL NOT NULL, y REAL NOT NULL,
            ts_utc TEXT NOT NULL, ts_epoch REAL NOT NULL,
            withdrawn INTEGER NOT NULL CHECK (withdrawn IN (0, 1)),
            upload_id TEXT NOT NULL REFERENCES uploads(upload_id),
            PRIMARY KEY (report_id, revision));
        INSERT INTO uploads VALUES ('old1', 'legacy.csv', '2026-11-18T00:00:00+00:00', 1);
        INSERT INTO reports VALUES ('L', 0, 'S1', 0, 0,
            '2026-11-19T10:00:00+00:00', 0, 0, 'old1');
        """
    )
    conn.commit()
    conn.close()

    db.init_db(db_file)
    conn = sqlite3.connect(db_file)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(uploads)")}
    conn.close()
    assert {"content_sha256", "is_fast", "superseded", "ingest_seq"} <= cols
    assert db.list_uploads(db_file)[0]["row_count"] == 1  # 数据保留

    text = make_csv(CHAIN)
    sha = hashlib.sha256(text.encode()).hexdigest()
    db.ingest_upload(db_file, "new.csv", core.parse_csv(text), sha)
    assert db.find_fast_source(db_file, sha) is not None
    fast = db.ingest_upload(db_file, "new-again.csv", None, sha)
    assert fast is not None and fast["mode"] == "fast"
    assert fast["row_count"] == 3
    assert len(db.get_effective_reports(db_file, "S1")) == 4  # L + A,B,C


def test_legacy_upload_with_same_bytes_not_fast_after_upgrade(tmp_path):
    """升级前入库的文件即使字节相同，首次重传也按正常入库，之后才可快传。"""
    db_file = str(tmp_path / "legacy2.db")
    db.init_db(db_file)
    text = make_csv(CHAIN)
    # 模拟旧库：直接写 uploads/reports，但不写 content_sha256
    conn = sqlite3.connect(db_file)
    conn.execute(
        "INSERT INTO uploads (upload_id, filename, uploaded_at, row_count) "
        "VALUES ('legacy', 'x.csv', '2026-11-18T00:00:00+00:00', 3)"
    )
    for r in core.parse_csv(text):
        conn.execute(
            "INSERT INTO reports VALUES (?,?,?,?,?,?,?,?,?)",
            (r.report_id, r.revision, r.segment_id, r.x, r.y, r.ts_utc,
             r.ts_epoch, int(r.withdrawn), "legacy"),
        )
    conn.commit()
    conn.close()

    app = create_app(db_file)
    with TestClient(app) as c:
        first = post(c, text, filename="again.csv")
        assert "上传成功" in first.text
        second = post(c, text, filename="again2.csv")
        assert "快速重传" in second.text
