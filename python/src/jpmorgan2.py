from typing import List
import math
import heapq

# ============================================================
#  YOU IMPLEMENT THIS.
#  Top-k filter: keep the k highest logits, softmax over ONLY
#  those k, all other positions get probability 0.0.
#  - Numerical stability: subtract max before exp.
#  - Ties at the k-th boundary: lower index wins.
#  - k <= 0  -> define behavior (return all zeros).
#  - k >= V  -> plain softmax over everything.
#  Return a length-V probability list (kept positions sum to 1).
# ============================================================
def top_k_filter(logits: List[float], k: int) -> List[float]:
    # TODO: your code here
    max_v =0
    for i, val in enumerate(logits):
        max_v =max(max_v, val)
        
    heap=[]
    new_logits =[]
    exp_logits =[]
    s =set()
    
    if k < len(logits):
        new_logits =[0 for i in range(len(logits))]
        if k<=0:
            return new_logits
        for i, val in enumerate(logits):
            heapq.heappush(heap, [val, -i])
            if len(heap) > k:
                heapq.heappop(heap)
        while len(heap) >0:
            x =heapq.heappop(heap)
            s.add(-x[1])
    else:
        s =set(range(len(logits)))
    exp_logits =[math.exp(val-max_v) if i in s else 0 for i, val in enumerate(logits)]
    
    
    exp_total = 0
    for val in exp_logits:
        exp_total +=val
    if exp_total ==0:
        exp_total += 0.00000001
    exp_logits =[val/exp_total for val in exp_logits]
    return exp_logits


# ============================================================
#  Test harness — do not edit below.
# ============================================================
def _run():
    passed = failed = 0
    def approx(a, b, tol=1e-6):
        return len(a) == len(b) and all(abs(x-y) < tol for x, y in zip(a, b))
    def check(name, logits, k, expected):
        nonlocal passed, failed
        got = top_k_filter(list(logits), k)
        ok = approx(got, expected)
        # also sanity: nonzero entries sum to 1 (when any kept)
        s = sum(got)
        sum_ok = (abs(s - 1.0) < 1e-6) or (s == 0.0)
        if ok and sum_ok:
            passed += 1
            print(f"  [PASS] {name} -> {[round(x,4) for x in got]}")
        else:
            failed += 1
            print(f"  [FAIL] {name}")
            print(f"         got      {[round(x,6) for x in got]}  (sum={s:.6f})")
            print(f"         expected {[round(x,6) for x in expected]}")

    # 1) spec example
    check("spec", [2.0,1.0,0.1,3.0], 2, [0.2689414, 0.0, 0.0, 0.7310586])

    # 2) overflow guard: huge logits must not blow up
    check("overflow", [1000.0, 999.0, 1.0], 2, [0.7310586, 0.2689414, 0.0])

    # 3) tie at boundary: two 3.0s, lower index wins -> index1 kept, index2 dropped
    check("tie_lower_idx", [5.0,3.0,3.0,1.0], 2, [0.8807971, 0.1192029, 0.0, 0.0])

    # 4) k >= V -> plain softmax over all
    check("k_ge_V", [0.0,0.0,0.0], 5, [1/3, 1/3, 1/3])

    # 5) k == V exactly
    check("k_eq_V", [1.0,2.0], 2, [0.2689414, 0.7310586])

    # 6) k == 1 -> argmax gets prob 1
    check("k_eq_1", [1.0,5.0,2.0], 1, [0.0, 1.0, 0.0])

    # 7) k <= 0 -> all zeros (defined behavior)
    check("k_zero", [1.0,2.0,3.0], 0, [0.0,0.0,0.0])

    # 8) negative logits handled
    check("negatives", [-1.0,-2.0,-3.0], 2, [0.7310586, 0.2689414, 0.0])

    print(f"\n==== {passed} passed, {failed} failed ====")
    return failed == 0

if __name__ == "__main__":
    import sys
    sys.exit(0 if _run() else 1)