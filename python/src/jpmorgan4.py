import numpy as np

# ============================================================
#  YOU IMPLEMENT THIS.
#  Multi-head scaled dot-product attention.
#    Q, K, V: [B, L, d_model]
#    h: number of heads (d_model % h == 0)
#  Steps:
#    1. split into h heads: [B,L,d_model] -> [B,h,L,d_k]
#    2. scaled dot-product per head: softmax(QK^T / sqrt(d_k)) @ V
#    3. concat heads back -> [B, L, d_model]
#  Return: [B, L, d_model]
# ============================================================
def multi_head_attention(Q, K, V, h):
    # TODO: your code here
    B,L,d_model =Q.shape
    d_k = d_model//h 
    assert d_model%h ==0
    Q = Q.reshape(B, L, h, d_k)
    Q = Q.transpose(0, 2, 1, 3)
    K = K.reshape(B, L, h, d_k)
    K = K.transpose(0, 2, 1, 3)
    V = V.reshape(B, L, h, d_k)
    V = V.transpose(0, 2, 1, 3)

    score = Q @ K.transpose(0, 1, 3, 2)
    score = score /np.sqrt(d_k)
    score = score - np.max(score, axis=-1, keepdims=True)
    exp_score =np.exp(score)
    exp_score = exp_score/np.sum(exp_score, axis=-1, keepdims=True)

    final =exp_score @ V
    final =final.transpose(0, 2, 1, 3)
    final =final.reshape(B, L, d_model)
    return final

# ============================================================
#  Test harness — do not edit below.
# ============================================================
def _run():
    passed = failed = 0
    def check(name, cond):
        nonlocal passed, failed
        if cond: passed += 1; print(f"  [PASS] {name}")
        else:    failed += 1; print(f"  [FAIL] {name}")

    np.random.seed(0)

    # 1) output shape
    B, L, d_model, h = 2, 4, 8, 2
    Q = np.random.randn(B, L, d_model)
    K = np.random.randn(B, L, d_model)
    V = np.random.randn(B, L, d_model)
    out = multi_head_attention(Q, K, V, h)
    check("output_shape [2,4,8]", out is not None and out.shape == (B, L, d_model))

    # 2) h=1 must equal plain single-head attention
    def single_head(Q, K, V):
        d = Q.shape[-1]
        s = Q @ K.transpose(0, 2, 1) / np.sqrt(d)
        s = s - s.max(-1, keepdims=True)
        w = np.exp(s); w /= w.sum(-1, keepdims=True)
        return w @ V
    out1 = multi_head_attention(Q, K, V, 1)
    ref1 = single_head(Q, K, V)
    check("h=1 == single-head", out1 is not None and np.allclose(out1, ref1))

    # 3) attention weights per head sum to 1 (implied by correct softmax)
    #    verify via a controlled input: if V rows identical, output rows == that V row
    Vconst = np.ones((B, L, d_model)) * 3.0
    outc = multi_head_attention(Q, K, Vconst, h)
    check("softmax_normalized (const V -> const out)",
          outc is not None and np.allclose(outc, 3.0))

    # 4) different h values all produce correct shape
    B2, L2, d2 = 1, 6, 12
    Q2 = np.random.randn(B2, L2, d2); K2 = np.random.randn(B2, L2, d2); V2 = np.random.randn(B2, L2, d2)
    ok_shapes = True
    for hh in [1, 2, 3, 4, 6, 12]:
        o = multi_head_attention(Q2, K2, V2, hh)
        if o is None or o.shape != (B2, L2, d2): ok_shapes = False
    check("all divisor heads give right shape", ok_shapes)

    # 5) numerical stability: large logits shouldn't produce nan/inf
    Qbig = np.random.randn(1, 3, 4) * 100
    Kbig = np.random.randn(1, 3, 4) * 100
    Vbig = np.random.randn(1, 3, 4)
    obig = multi_head_attention(Qbig, Kbig, Vbig, 2)
    check("no nan/inf on large logits",
          obig is not None and np.all(np.isfinite(obig)))

    # 6) output is a proper convex combination of V rows (each output row within V's range per dim)
    #    since attention output = weighted avg of V rows, each dim must lie within [min,max] of V that head
    outr = multi_head_attention(Q, K, V, h)
    within = True
    if outr is not None:
        vmin = V.min(axis=1, keepdims=True)  # [B,1,d_model]
        vmax = V.max(axis=1, keepdims=True)
        # allow tiny epsilon
        if not (np.all(outr >= vmin - 1e-9) and np.all(outr <= vmax + 1e-9)):
            within = False
    check("output is convex combo of V rows", outr is not None and within)

    print(f"\n==== {passed} passed, {failed} failed ====")
    return failed == 0

if __name__ == "__main__":
    import sys
    sys.exit(0 if _run() else 1)