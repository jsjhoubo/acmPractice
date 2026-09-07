"""Event-pipeline utilities. Docstrings are the spec.
Several components contain bugs; make tests.py pass WITHOUT editing tests.py."""
import numpy as np
from bisect import bisect_left, bisect_right


class TokenBucket:
    """Rate limiter. capacity: max tokens; refill_rate: tokens/second.
    allow(t, cost): refill by elapsed*rate (capped at capacity), then spend."""
    def __init__(self, capacity, refill_rate):
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self.tokens = float(capacity)
        self.last = 0.0

    def allow(self, t, cost=1.0):
        elapsed = t - self.last
        self.last = t
        self.tokens = self.tokens + elapsed * self.refill_rate
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


class EventStore:
    """Append-only store of (t, v) with nondecreasing t."""
    def __init__(self):
        self.times = []
        self.values = []

    def add(self, t, v):
        self.times.append(float(t))
        self.values.append(float(v))

    def query_sum(self, t0, t1):
        """Sum of values with t in the INCLUSIVE range [t0, t1]."""
        i = bisect_left(self.times, t0)
        j = bisect_left(self.times, t1)
        return float(sum(self.values[i:j]))

    def query_count(self, t0, t1):
        i = bisect_left(self.times, t0)
        j = bisect_right(self.times, t1)
        return j - i


def sessionize(times, gap):
    """Sorted timestamps -> list of (start, end) sessions.
    A new session starts when the delta from the previous timestamp is > gap
    (delta == gap stays in the same session). times is non-empty."""
    sessions = []
    start = prev = times[0]
    for t in times[1:]:
        if t - prev > gap:
            sessions.append((start, prev))
            start = t
        prev = t
    return sessions


def percentile(x, p):
    """Linear-interpolation percentile identical to np.percentile."""
    xs = sorted(float(v) for v in x)
    idx = min(len(xs) - 1, int(len(xs) * p / 100.0))
    return xs[idx]


def parse_kv_log(line):
    """Parse 'k1=v1 k2=v2 ... msg=free text with spaces' into a dict.
    Every field before msg contains no spaces; msg (if present) is LAST and
    its value is the entire remainder of the line."""
    out = {}
    for part in line.split():
        k, v = part.split("=", 1)
        out[k] = v
    return out


class TTLCache:
    """set(k, v, t) stores; get(k, t) returns v while t - t_set < ttl, else
    None (expired entries are evicted on access)."""
    def __init__(self, ttl):
        self.ttl = float(ttl)
        self.data = {}

    def set(self, key, value, t):
        self.data[key] = (value, float(t))

    def get(self, key, t):
        if key not in self.data:
            return None
        value, t0 = self.data[key]
        if t - t0 <= self.ttl:
            return value
        del self.data[key]
        return None


def backoff_delays(base, factor, retries, cap):
    """Delay before retry i (0-indexed): min(cap, base * factor**i)."""
    return [float(min(cap, base * factor ** i)) for i in range(retries)]


def robust_std(x):
    """Sample standard deviation (ddof=1); fewer than 2 points -> 0.0."""
    x = np.asarray(x, dtype=float)
    if x.size < 2:
        return 0.0
    return float(np.std(x))


def top_events(counts, k):
    """Top-k (name, count): count desc, ties name asc."""
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:k]


def normalize01(x):
    """Scale to [0,1]; constant arrays -> zeros."""
    x = np.asarray(x, dtype=float)
    lo, hi = float(np.min(x)), float(np.max(x))
    if hi == lo:
        return np.zeros_like(x)
    return (x - lo) / (hi - lo)


def ewma(x, alpha):
    """Exponentially weighted moving average: y0=x0; yi = alpha*xi+(1-alpha)*y(i-1)."""
    out = []
    y = None
    for v in x:
        y = float(v) if y is None else alpha * float(v) + (1 - alpha) * y
        out.append(y)
    return out


def dedupe_preserve_order(items):
    """Remove duplicates keeping first occurrence order."""
    seen = set()
    out = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def chunk_by_size(items, limit, size_of):
    """Greedy chunking: pack items in order; start a new chunk when adding the
    next item would exceed `limit` (a single oversize item gets its own chunk)."""
    chunks = []
    cur = []
    cur_sz = 0.0
    for it in items:
        s = float(size_of(it))
        if cur and cur_sz + s > limit:
            chunks.append(cur)
            cur = []
            cur_sz = 0.0
        cur.append(it)
        cur_sz += s
    if cur:
        chunks.append(cur)
    return chunks
