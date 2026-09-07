"""User activity telemetry utilities.

Each function's docstring is its spec. The test suite (tests.py) encodes the
same spec. Several functions contain bugs: make all tests pass WITHOUT
changing tests.py.
"""
from collections import namedtuple
import numpy as np

Record = namedtuple("Record", ["user", "day", "value"])


def parse_records(lines):
    """Parse CSV lines "user,day,value" -> list[Record].

    day is an int, value is a float. Blank lines are skipped.
    """
    records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        records.append(Record(parts[0], int(parts[1]), float(parts[2])))
    return records


def daily_totals(records, n_days):
    """Sum of `value` per day, as a float array of length exactly n_days.

    Days with no records contribute 0. All record days are in [0, n_days).
    """
    days = np.array([r.day for r in records], dtype=int)
    vals = np.array([r.value for r in records], dtype=float)
    if len(records) == 0:
        return np.zeros(n_days)
    return np.bincount(days, weights=vals)


def moving_average(x, w):
    """Sliding-window mean: out[i] = mean(x[i : i+w]).

    Returns an array of length len(x) - w + 1. Assumes 1 <= w <= len(x).
    """
    x = np.asarray(x, dtype=float)
    c = np.cumsum(x)
    return (c[w:] - c[:-w]) / w


def clean_stats(x):
    """Return (mean, max) of x ignoring NaN entries.

    If x is empty or all-NaN, return (0.0, 0.0).
    """
    x = np.asarray(x, dtype=float)
    x = x[x != np.nan]
    if x.size == 0:
        return (0.0, 0.0)
    return (float(np.mean(x)), float(np.max(x)))


def top_k_users(totals, k):
    """Top-k (user, total) pairs: total descending, ties by user ascending."""
    items = sorted(totals.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    return items[:k]


def count_sessions(node):
    """Total sessions in the tree rooted at `node`.

    node = {"sessions": int, "children": [child nodes...]} ("children" may
    be absent).
    """
    total = node["sessions"]
    for child in node.get("children", []):
        count_sessions(child)
    return total


def sample_users(users, n, seed):
    """Return n distinct users chosen pseudo-randomly.

    Deterministic for a given seed. MUST NOT modify the input list.
    """
    rng = np.random.RandomState(seed)
    rng.shuffle(users)
    return users[:n]


def normalize(x):
    """Scale x to [0, 1]. Constant arrays map to all zeros."""
    x = np.asarray(x, dtype=float)
    lo, hi = np.min(x), np.max(x)
    if hi == lo:
        return np.zeros_like(x)
    return (x - lo) / (hi - lo)
