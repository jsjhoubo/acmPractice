import numpy as np

# ============================================================
#  YOU IMPLEMENT BOTH.
#  Softmax + Cross-Entropy loss for a batch of logits.
#    logits: [B, C]  (B samples, C classes)
#    labels: [B]     (integer class index in [0, C-1])
#
#  forward: return scalar loss = mean over batch of
#           -log( softmax(logits)[correct_class] )
#           (numerically stable softmax: subtract max)
#           also return cache for backward.
#
#  backward: return dlogits [B, C] = d(loss)/d(logits)
#           (the famous simplification: (softmax - onehot)/B )
# ============================================================
def softmax_ce_forward(logits, labels):
    # TODO
    loss = None
    cache = None
    return loss, cache

def softmax_ce_backward(cache):
    # TODO
    dlogits = None
    return dlogits


# ============================================================
#  Test harness — do not edit below.
# ============================================================
def _run():
    passed = failed = 0
    def check(name, cond, extra=""):
        nonlocal passed, failed
        if cond: passed += 1; print(f"  [PASS] {name} {extra}")
        else:    failed += 1; print(f"  [FAIL] {name} {extra}")

    np.random.seed(0)
    B, C = 5, 4
    logits = np.random.randn(B, C)
    labels = np.random.randint(0, C, size=B)

    # forward correctness vs reference
    loss, cache = softmax_ce_forward(logits, labels)
    if loss is not None:
        # reference
        z = logits - logits.max(1, keepdims=True)
        p = np.exp(z); p /= p.sum(1, keepdims=True)
        ref = -np.mean(np.log(p[np.arange(B), labels] + 1e-12))
        check("forward matches reference", np.isclose(loss, ref, atol=1e-6),
              f"loss={loss:.4f} ref={ref:.4f}")

        # perfect prediction -> ~0 loss
        big = np.full((3,3), -10.0); big[np.arange(3),[0,1,2]] = 10.0
        lp,_ = softmax_ce_forward(big, np.array([0,1,2]))
        check("confident correct -> low loss", lp < 0.01, f"loss={lp:.4f}")

        # uniform logits -> loss = log(C)
        uni = np.zeros((2, C))
        lu,_ = softmax_ce_forward(uni, np.array([0,1]))
        check("uniform -> log(C)", np.isclose(lu, np.log(C), atol=1e-6), f"loss={lu:.4f}")

        # stability: huge logits, no overflow
        huge = np.random.randn(2,C)*1000
        lh,_ = softmax_ce_forward(huge, np.array([0,1]))
        check("no overflow on huge logits", np.isfinite(lh))
    else:
        check("forward matches reference", False, "(None)")

    # backward via numerical gradient
    loss, cache = softmax_ce_forward(logits, labels)
    dlogits = softmax_ce_backward(cache)
    if dlogits is not None:
        num = np.zeros_like(logits)
        h = 1e-6
        it = np.nditer(logits, flags=['multi_index'])
        while not it.finished:
            i = it.multi_index
            old = logits[i]
            logits[i] = old + h; fp,_ = softmax_ce_forward(logits, labels)
            logits[i] = old - h; fm,_ = softmax_ce_forward(logits, labels)
            logits[i] = old
            num[i] = (fp - fm)/(2*h)
            it.iternext()
        check("dlogits matches numerical", np.allclose(dlogits, num, atol=1e-5),
              f"maxerr={np.max(np.abs(dlogits-num)):.2e}")
        # gradient should be (softmax - onehot)/B
        z = logits - logits.max(1, keepdims=True)
        p = np.exp(z); p /= p.sum(1, keepdims=True)
        onehot = np.zeros_like(p); onehot[np.arange(B), labels] = 1
        expected = (p - onehot)/B
        check("dlogits == (softmax-onehot)/B", np.allclose(dlogits, expected, atol=1e-9))
    else:
        check("dlogits matches numerical", False, "(None)")

    print(f"\n==== {passed} passed, {failed} failed ====")
    return failed == 0

if __name__ == "__main__":
    import sys
    sys.exit(0 if _run() else 1)