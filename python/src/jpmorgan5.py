import numpy as np

# ============================================================
#  YOU IMPLEMENT BOTH.
#  LayerNorm over the FEATURE dim (per-sample, not per-batch).
#    x: [B, D]   gamma,beta: [D]
#    forward: y = gamma * (x-mu)/sqrt(var+eps) + beta
#             mu, var computed over axis=-1 (each row independent)
#    return: out [B,D], cache (whatever backward needs)
# ============================================================
def layernorm_forward(x, gamma, beta, eps=1e-5):
    # TODO
    mu =np.mean(x, axis=-1, keepdims=True)
    var =np.var(x, axis =-1, keepdims=True)
    out = (x -mu)/np.sqrt(var + eps) * gamma + beta
    cache = [ gamma * 1/np.sqrt(var + eps),  (x -mu)/np.sqrt(var + eps) ]
    return out, cache

# ============================================================
#  backward: given dout [B,D], return dx [B,D], dgamma [D], dbeta [D]
#  (mu and var both depend on x -> gradient flows through 3 paths)
# ============================================================
def layernorm_backward(dout, cache):
    # TODO
    dx = cache[0] * dout
    dgamma = np.sum(dout * cache[1], axis=0)
    dbeta = np.sum(dout, axis=0)
    return dx, dgamma, dbeta


# ============================================================
#  Test harness — do not edit below.
#  Verifies forward correctness + backward via NUMERICAL gradient.
# ============================================================
def _run():
    passed = failed = 0
    def check(name, cond, extra=""):
        nonlocal passed, failed
        if cond: passed += 1; print(f"  [PASS] {name} {extra}")
        else:    failed += 1; print(f"  [FAIL] {name} {extra}")

    np.random.seed(0)
    B, D = 4, 5
    x = np.random.randn(B, D)
    gamma = np.random.randn(D)
    beta = np.random.randn(D)
    eps = 1e-5

    # --- forward correctness ---
    out, cache = layernorm_forward(x, gamma, beta, eps)
    if out is not None:
        mu = x.mean(-1, keepdims=True)
        var = x.var(-1, keepdims=True)
        ref = gamma * (x - mu)/np.sqrt(var+eps) + beta
        check("forward matches reference", np.allclose(out, ref))
        # normalized rows (gamma=1,beta=0) have ~0 mean, ~1 var
        o2,_ = layernorm_forward(x, np.ones(D), np.zeros(D), eps)
        check("normalized: row mean ~0", np.allclose(o2.mean(-1), 0, atol=1e-6))
        check("normalized: row var ~1", np.allclose(o2.var(-1), 1, atol=1e-2))
    else:
        check("forward matches reference", False, "(returned None)")

    # --- backward via numerical gradient ---
    dout = np.random.randn(B, D)
    out, cache = layernorm_forward(x, gamma, beta, eps)
    dx, dgamma, dbeta = layernorm_backward(dout, cache)

    def num_grad(f, w, h=1e-6):
        g = np.zeros_like(w)
        it = np.nditer(w, flags=['multi_index'])
        while not it.finished:
            i = it.multi_index
            old = w[i]
            w[i] = old + h; fp = f()
            w[i] = old - h; fm = f()
            w[i] = old
            g[i] = np.sum((fp - fm) * dout) / (2*h)
            it.iternext()
        return g

    if dx is not None:
        # numerical dx
        xg = x.copy()
        ndx = num_grad(lambda: layernorm_forward(xg, gamma, beta, eps)[0], xg)
        check("dx matches numerical", np.allclose(dx, ndx, atol=1e-5),
              f"maxerr={np.max(np.abs(dx-ndx)):.2e}")
    else:
        check("dx matches numerical", False, "(returned None)")

    if dgamma is not None:
        gg = gamma.copy()
        ndg = num_grad(lambda: layernorm_forward(x, gg, beta, eps)[0], gg)
        check("dgamma matches numerical", np.allclose(dgamma, ndg, atol=1e-5),
              f"maxerr={np.max(np.abs(dgamma-ndg)):.2e}")
    else:
        check("dgamma matches numerical", False, "(returned None)")

    if dbeta is not None:
        bb = beta.copy()
        ndb = num_grad(lambda: layernorm_forward(x, gamma, bb, eps)[0], bb)
        check("dbeta matches numerical", np.allclose(dbeta, ndb, atol=1e-5),
              f"maxerr={np.max(np.abs(dbeta-ndb)):.2e}")
    else:
        check("dbeta matches numerical", False, "(returned None)")

    print(f"\n==== {passed} passed, {failed} failed ====")
    return failed == 0

if __name__ == "__main__":
    import sys
    sys.exit(0 if _run() else 1)