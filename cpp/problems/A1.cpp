#include <vector>
#include <iostream>
#include <algorithm>
#include <numeric>
#include <cassert>
#include <random>
#include <cmath>
using namespace std;
struct Chunk {
    int id;
    double score;
    int tokens;
};

std::vector<int> selectChunks(const std::vector<Chunk>& chunks, int budget) {

    int n =chunks.size();
    if (n==0 || budget<=0) {
        return {};
    }
    vector<vector<double>> f(n, vector<double>(budget+1, 0.0));
    vector<vector<int>> pre(n, vector<int>(budget+1, -1));
    for(int j=chunks[0].tokens;j<=budget;j++) {
        f[0][j] =chunks[0].score;
        pre[0][j] =0;
    }
    for (int i=1;i<n;i++) {
        for (int j=0;j<=budget;j++) {
            f[i][j] =f[i-1][j];
            double t = j>=chunks[i].tokens ? f[i-1][j -chunks[i].tokens] + chunks[i].score : 0;
            if (t > f[i][j]) {
                pre[i][j] = i;
                f[i][j] =t;
            }

        }
    }
    vector<int> selected;
    int j =budget;
    for (int i=n-1;i>=0;i--) {
        if (pre[i][j] >=0) {
            int x =pre[i][j];
            selected.push_back(chunks[x].id);
            j -= chunks[x].tokens;
        }
    }
    reverse(selected.begin(), selected.end());
    return selected;
}


// ==== brute-force oracle: 枚举所有子集, 返回最优总 score ====
// 仅用于小 N 对拍 (N <= ~20)
double bruteForceBest(const std::vector<Chunk>& chunks, int budget) {
    int n = chunks.size();
    double best = 0.0;
    for (int mask = 0; mask < (1 << n); ++mask) {
        int tok = 0;
        double sc = 0.0;
        for (int i = 0; i < n; ++i) {
            if (mask & (1 << i)) { tok += chunks[i].tokens; sc += chunks[i].score; }
        }
        if (tok <= budget) best = std::max(best, sc);
    }
    return best;
}

// 把 selectChunks 返回的 id 列表换算成 (总score, 总token), 并校验合法性
std::pair<double,int> scoreOf(const std::vector<Chunk>& chunks,
                              const std::vector<int>& picked) {
    // id -> index 映射
    std::vector<int> seen;
    double sc = 0.0; int tok = 0;
    for (int id : picked) {
        bool found = false;
        for (const auto& c : chunks) {
            if (c.id == id) { sc += c.score; tok += c.tokens; found = true; break; }
        }
        assert(found && "returned id not in input");
        // 查重: 同一个 id 不应被选两次
        assert(std::find(seen.begin(), seen.end(), id) == seen.end() && "duplicate id");
        seen.push_back(id);
    }
    return {sc, tok};
}

const double EPS = 1e-6;

void runCase(const std::string& name,
             const std::vector<Chunk>& chunks, int budget) {
    auto picked = selectChunks(chunks, budget);
    auto [sc, tok] = scoreOf(chunks, picked);
    double opt = bruteForceBest(chunks, budget);

    bool budgetOk = (tok <= budget);
    bool optimalOk = (std::abs(sc - opt) < EPS);

    std::cout << "[" << (budgetOk && optimalOk ? "PASS" : "FAIL") << "] "
              << name
              << "  picked_score=" << sc
              << "  tokens=" << tok << "/" << budget
              << "  oracle_opt=" << opt << "\n";
    if (!budgetOk)  std::cout << "    !! over budget\n";
    if (!optimalOk) std::cout << "    !! not optimal (got " << sc << ", want " << opt << ")\n";
}

int main() {
    // --- 边界 / 退化 ---
    runCase("empty input",            {}, 100);
    runCase("budget 0",               {{1, 5.0, 3}, {2, 2.0, 1}}, 0);
    runCase("single fits",            {{1, 5.0, 3}}, 5);
    runCase("single too big",         {{1, 5.0, 10}}, 5);
    runCase("all fit",                {{1, 1.0, 1}, {2, 2.0, 1}, {3, 3.0, 1}}, 100);

    // --- 经典权衡: 贪心(密度) 会翻车的反例 ---
    // 贪心按 score/token: item1 密度 6/1=6 最高, 但选它就放不下 5+5
    // 最优是 item2+item3 = 10 (tokens 8 <= 10), 而 item1=6
    runCase("greedy trap",
            {{1, 6.0, 1}, {2, 5.0, 4}, {3, 5.0, 4}}, 8);

    // --- 恰好填满 budget ---
    runCase("exact fill",
            {{1, 3.0, 5}, {2, 4.0, 5}, {3, 1.0, 3}}, 10);

    // --- double score 精度 (不能当下标的证明: score 只累加) ---
    runCase("double scores",
            {{1, 1.5, 2}, {2, 2.7, 3}, {3, 0.3, 1}, {4, 4.9, 4}}, 6);

    // --- 重复 tokens 不同 score ---
    runCase("same tokens",
            {{1, 1.0, 2}, {2, 5.0, 2}, {3, 3.0, 2}}, 4);

    // --- 随机对拍: 小 N, 拿 oracle 兜底 ---
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> nDist(0, 14);
    std::uniform_int_distribution<int> tokDist(1, 12);
    std::uniform_real_distribution<double> scDist(0.0, 10.0);
    std::uniform_int_distribution<int> budDist(0, 40);

    int trials = 2000, passed = 0;
    for (int t = 0; t < trials; ++t) {
        int n = nDist(rng);
        std::vector<Chunk> v;
        for (int i = 0; i < n; ++i)
            v.push_back({i, std::round(scDist(rng)*10)/10.0, tokDist(rng)});
        int budget = budDist(rng);

        auto picked = selectChunks(v, budget);
        auto [sc, tok] = scoreOf(v, picked);
        double opt = bruteForceBest(v, budget);
        if (tok <= budget && std::abs(sc - opt) < EPS) ++passed;
        else {
            std::cout << "[FAIL] random trial " << t
                      << " n=" << n << " budget=" << budget
                      << " got=" << sc << " want=" << opt << "\n";
        }
    }
    std::cout << "random oracle: " << passed << "/" << trials << " passed\n";

    return 0;
}