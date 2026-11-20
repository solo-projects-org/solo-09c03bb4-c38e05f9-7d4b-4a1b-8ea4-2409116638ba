"""核心业务逻辑：CSV 解析校验、连通图构建与桥接（割点）识别。

本模块不依赖 FastAPI / SQLite，全部为纯函数，便于单独测试。

判定规则（与需求一致）：
- 同一路段、两条【未撤回】上报，平面距离 <= 5 米 且 UTC 时差 <= 10 分钟，则相连；
- 某条上报被移除后，其所在连通组分裂成至少两个非空组，该上报才是疑似桥接
  （即图论中的割点/关节点）；普通边缘点（叶子）移除后只剩一个组，不算桥接。
"""
from __future__ import annotations

import csv
import io
import math
from dataclasses import dataclass
from datetime import datetime, timezone

MAX_DISTANCE_M = 5.0  # 相距不超过 5 米
MAX_TIME_DIFF_S = 600.0  # 时差不超过 10 分钟


class CsvError(Exception):
    """CSV 校验失败。消息中可包含多行错误描述。"""


@dataclass(frozen=True)
class ReportRow:
    report_id: str
    revision: int
    segment_id: str
    x: float
    y: float
    ts_utc: str  # 归一化后的 UTC ISO-8601
    ts_epoch: float  # UTC 秒时间戳，用于时差计算
    withdrawn: bool


_HEADER_ALIASES = {
    "report_id": ("report_id", "上报号"),
    "revision": ("revision", "修订号"),
    "segment_id": ("segment_id", "路段号"),
    "x": ("x", "横坐标"),
    "y": ("y", "纵坐标"),
    "ts_utc": ("ts_utc", "utc时刻", "时刻"),
    "withdrawn": ("withdrawn", "撤回标记"),
}

_TRUE_VALUES = {"1", "true", "t", "yes", "y", "是"}
_FALSE_VALUES = {"0", "false", "f", "no", "n", "", "否"}


def _canon_header(name: str) -> str:
    return name.strip().lstrip("\ufeff").lower()


def _map_header(header: list[str]) -> dict[str, int]:
    alias = {}
    for canon, names in _HEADER_ALIASES.items():
        for n in names:
            alias[n] = canon
    colmap: dict[str, int] = {}
    errors: list[str] = []
    for idx, raw in enumerate(header):
        key = _canon_header(raw)
        if not key:
            continue
        canon = alias.get(key)
        if canon is None:
            errors.append(f"未知列名 {raw!r}")
        elif canon in colmap:
            errors.append(f"列 {canon!r} 在表头重复出现")
        else:
            colmap[canon] = idx
    missing = [c for c in _HEADER_ALIASES if c not in colmap]
    if missing:
        errors.append("缺少必需列: " + ", ".join(missing))
    if errors:
        allowed = " / ".join("/".join(v) for v in _HEADER_ALIASES.values())
        raise CsvError("表头错误：" + "；".join(errors) + f"。允许的列名：{allowed}")
    return colmap


def _parse_bool(value: str) -> bool:
    v = value.strip().lower()
    if v in _TRUE_VALUES:
        return True
    if v in _FALSE_VALUES:
        return False
    raise ValueError(f"撤回标记无法识别: {value!r}（可用 0/1、true/false、yes/no，留空视为未撤回）")


def _parse_ts(value: str) -> tuple[str, float]:
    v = value.strip()
    if not v:
        raise ValueError("UTC 时刻为空")
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        raise ValueError(f"UTC 时刻无法解析: {value!r}（需 ISO 8601，如 2026-11-19T08:30:00Z）")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # 无时区按 UTC 处理
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat(), dt.timestamp()


def _parse_row(raw: list[str], colmap: dict[str, int]) -> ReportRow:
    def cell(canon: str) -> str:
        return raw[colmap[canon]].strip()

    report_id = cell("report_id")
    if not report_id:
        raise ValueError("上报号为空")
    segment_id = cell("segment_id")
    if not segment_id:
        raise ValueError("路段号为空")
    rev_raw = cell("revision")
    try:
        revision = int(rev_raw)
    except ValueError:
        raise ValueError(f"修订号不是整数: {rev_raw!r}")
    if revision < 0:
        raise ValueError(f"修订号不能为负: {revision}")
    try:
        x = float(cell("x"))
        y = float(cell("y"))
    except ValueError:
        raise ValueError(f"平面米坐标无法解析: {cell('x')!r}, {cell('y')!r}")
    if not (math.isfinite(x) and math.isfinite(y)):
        raise ValueError("平面米坐标必须是有限数值")
    ts_utc, ts_epoch = _parse_ts(cell("ts_utc"))
    withdrawn = _parse_bool(cell("withdrawn"))
    return ReportRow(report_id, revision, segment_id, x, y, ts_utc, ts_epoch, withdrawn)


def parse_csv(text: str) -> list[ReportRow]:
    """解析并校验整份 CSV。任何一行有问题都抛 CsvError（调用方应整体拒绝，
    不允许部分提交）。"""
    reader = csv.reader(io.StringIO(text))
    header = None
    for raw in reader:
        if raw and any(c.strip() for c in raw):
            header = raw
            break
    if header is None:
        raise CsvError("CSV 为空：缺少表头行")
    colmap = _map_header(header)
    width = len(header)

    rows: list[ReportRow] = []
    errors: list[str] = []
    seen: set[tuple[str, int]] = set()
    for raw in reader:
        if not raw or all(not c.strip() for c in raw):
            continue
        lineno = reader.line_num
        if len(raw) != width:
            errors.append(f"第 {lineno} 行：列数 {len(raw)} 与表头 {width} 不一致")
            continue
        try:
            row = _parse_row(raw, colmap)
        except ValueError as e:
            errors.append(f"第 {lineno} 行：{e}")
            continue
        key = (row.report_id, row.revision)
        if key in seen:
            errors.append(f"第 {lineno} 行：同一文件内重复的上报号+修订号 {key[0]}#{key[1]}")
            continue
        seen.add(key)
        rows.append(row)
    if errors:
        shown = "；".join(errors[:20])
        if len(errors) > 20:
            shown += f"；…共 {len(errors)} 处错误"
        raise CsvError(shown)
    if not rows:
        raise CsvError("CSV 中没有有效数据行")
    return rows


# ---------------------------------------------------------------- 图与桥接


def connected(a, b) -> bool:
    """两条上报是否相连：距离 <= 5 米 且 时差 <= 10 分钟（含边界）。"""
    d = math.hypot(a["x"] - b["x"], a["y"] - b["y"])
    dt = abs(a["ts_epoch"] - b["ts_epoch"])
    return d <= MAX_DISTANCE_M and dt <= MAX_TIME_DIFF_S


def build_adjacency(reports) -> dict[str, set[str]]:
    ids = [r["report_id"] for r in reports]
    adj: dict[str, set[str]] = {rid: set() for rid in ids}
    n = len(reports)
    for i in range(n):
        for j in range(i + 1, n):
            if connected(reports[i], reports[j]):
                a, b = reports[i]["report_id"], reports[j]["report_id"]
                adj[a].add(b)
                adj[b].add(a)
    return adj


def _articulation_points(ids: list[str], adj: dict[str, set[str]]) -> set[str]:
    """Tarjan 割点算法（迭代实现，避免递归深度限制）。"""
    disc: dict[str, int] = {}
    low: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    aps: set[str] = set()
    timer = 0
    for s in ids:
        if s in disc:
            continue
        parent[s] = None
        disc[s] = low[s] = timer
        timer += 1
        root_children = 0
        stack = [(s, iter(adj[s]))]
        while stack:
            v, it = stack[-1]
            descended = False
            for w in it:
                if w not in disc:
                    parent[w] = v
                    disc[w] = low[w] = timer
                    timer += 1
                    if parent[v] is None:
                        root_children += 1
                    stack.append((w, iter(adj[w])))
                    descended = True
                    break
                if w != parent[v]:
                    low[v] = min(low[v], disc[w])
            if descended:
                continue
            stack.pop()
            if stack:
                p = stack[-1][0]
                low[p] = min(low[p], low[v])
                if parent[p] is not None and low[v] >= disc[p]:
                    aps.add(p)
        if root_children > 1:
            aps.add(s)
    return aps


def _components(ids: list[str], adj: dict[str, set[str]]) -> list[list[str]]:
    seen: set[str] = set()
    comps: list[list[str]] = []
    for rid in ids:
        if rid in seen:
            continue
        comp = []
        stack = [rid]
        seen.add(rid)
        while stack:
            u = stack.pop()
            comp.append(u)
            for w in adj[u]:
                if w not in seen:
                    seen.add(w)
                    stack.append(w)
        comps.append(comp)
    return comps


def _split_groups(members: list[str], adj: dict[str, set[str]], removed: str) -> list[list[str]]:
    """在 removed 所属的连通组内移除它，求剩余节点的分组（组内上报号排序）。"""
    seen = {removed}
    groups: list[list[str]] = []
    for rid in members:
        if rid in seen:
            continue
        comp = []
        stack = [rid]
        seen.add(rid)
        while stack:
            u = stack.pop()
            comp.append(u)
            for w in adj[u]:
                if w not in seen:
                    seen.add(w)
                    stack.append(w)
        groups.append(sorted(comp))
    groups.sort()
    return groups


def find_bridges(reports) -> list[dict]:
    """找出疑似桥接上报。

    输入：同一路段、未撤回的有效上报，元素为含 report_id/x/y/ts_epoch 的映射。
    输出：[{"report_id": ..., "groups": [[上报号...], ...]}]，按上报号排序；
    仅包含“移除后原连通组分裂成至少两个非空组”的上报，并给出分裂后各组名单。
    """
    ids = [r["report_id"] for r in reports]
    adj = build_adjacency(reports)
    comp_of: dict[str, list[str]] = {}
    for comp in _components(ids, adj):
        for rid in comp:
            comp_of[rid] = comp
    result = []
    for ap in sorted(_articulation_points(ids, adj)):
        # 只统计“原连通组”的分裂；图中其他不相干连通组不算在内，
        # 否则叶子点会被误判成桥接
        groups = _split_groups(comp_of[ap], adj, ap)
        if len(groups) >= 2:  # 至少两个非空组才算桥接；叶子点移除后只剩一组，排除
            result.append({"report_id": ap, "groups": groups})
    return result
