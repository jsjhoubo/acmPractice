import heapq



def beam_step(
    beams: list[tuple[float, list[int]]],   # 当前 k 条 beam: (累积log_prob, token序列)
    next_log_probs: list[list[float]],       # next_log_probs[b][t] = beam b 接 token t 的 log_prob
    k: int
) -> list[tuple[float, list[int]]]:
    # 对每条 beam 扩展所有 token，保留全局 top-k 条新 beam
    # 返回新的 k 条: (新累积log_prob, 扩展后的token序列)，按 log_prob 降序
    ...
    next =[]
    for i, (log_prob, b_list) in enumerate(beams):
        for j, val in enumerate(next_log_probs[i]):    
            nb_list = b_list +[j]
            next.append((log_prob + val, nb_list))
    ret =heapq.nlargest(k, next, key=lambda x: x[0])
    return ret

beams = [(-1.0, [5]), (-1.5, [7])]   # 2 条 beam
next_log_probs = [
    [-0.5, -2.0],    # beam0 接 token0/token1 的 log_prob
    [-0.3, -1.0],    # beam1 接 token0/token1
]
k = 2
print()
# 候选: (-1.0-0.5,[5,0])=-1.5, (-1.0-2.0,[5,1])=-3.0,
#       (-1.5-0.3,[7,0])=-1.8, (-1.5-1.0,[7,1])=-2.5
# top-2: (-1.5,[5,0]), (-1.8,[7,0])