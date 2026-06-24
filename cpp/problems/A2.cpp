#include <vector>
#include <iostream>
#include <queue>
#include <unordered_set>
#include <algorithm>
#include <cassert>
#include <random>
#include <cmath>
using namespace std;

struct Result {
    int doc_id;
    double score;
};

// ============================================================
// 你来实现这个函数。streams[i] 已按 score 降序。
// 返回全局按 score 降序的 top-N 个 doc_id，同一 doc 只保留一次（取最高分）。
// ============================================================
std::vector<int> mergeTopN(const std::vector<std::vector<Result>>& streams, int N) {

    // TODO: your implementation
    auto cmp =[&](pair<size_t, size_t> &x, pair<size_t, size_t> &y){
        return streams[x.first][x.second].score < streams[y.first][y.second].score;
    };
    priority_queue<pair<size_t, size_t>, vector<pair<size_t, size_t>>, decltype(cmp)> pq(cmp);
    size_t m = streams.size();
    unordered_set<int> ids;
    for (size_t i=0;i<m;i++) {
        if (streams[i].size() >0) {
            pq.push({i, 0});
        }
    }
    vector<int> ret;
    while (!pq.empty()) {
        auto [i, j] = pq.top();
        pq.pop();
        if (ret.size() == (size_t)N) {
            break;
        }
        if (ids.find(streams[i][j].doc_id) ==ids.end()) {
            ret.push_back(streams[i][j].doc_id);
            ids.insert(streams[i][j].doc_id);
        }
        if (j+1<streams[i].size()) {
            pq.push({i, j+1});
        }
    }
    return ret;
}


// ==== brute-force oracle: 全部摊平、去重取最高分、排序、取 top-N ====
std::vector<int> bruteForce(const std::vector<std::vector<Result>>& streams, int N) {
    // doc_id -> 最高 score
    vector<pair<int,double>> best; // (doc_id, best_score)
    auto findIdx = [&](int id) -> int {
        for (int i = 0; i < (int)best.size(); ++i) if (best[i].first == id) return i;
        return -1;
    };
    for (const auto& s : streams) {
        for (const auto& r : s) {
            int idx = findIdx(r.doc_id);
            if (idx == -1) best.push_back({r.doc_id, r.score});
            else if (r.score > best[idx].second) best[idx].second = r.score;
        }
    }
    // 按 score 降序；score 相等时按 doc_id 升序（保证确定性）
    sort(best.begin(), best.end(), [](const pair<int,double>& a, const pair<int,double>& b){
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });
    vector<int> out;
    for (int i = 0; i < (int)best.size() && i < N; ++i) out.push_back(best[i].first);
    return out;
}

const double EPS = 1e-9;

// 校验: 你的结果 vs oracle。
// 注意: 当存在 score 并列时, top-N 边界上选哪个 doc 可能有歧义,
// 所以这里校验两件事 (而不是逐位置死等):
//   (1) 长度一致
//   (2) 你选出的每个 doc, 其最高分组成的"分数多重集"与 oracle 一致
//       (即你没选错分数档位的 doc)
bool validate(const vector<int>& got,
              const vector<int>& want,
              const vector<vector<Result>>& streams) {
    if (got.size() != want.size()) return false;

    // doc_id -> 最高分
    auto bestScore = [&](int id) -> double {
        double b = -1e18; bool found = false;
        for (auto& s : streams) for (auto& r : s)
            if (r.doc_id == id) { b = max(b, r.score); found = true; }
        return found ? b : -1e18;
    };

    // got 里不应有重复 doc
    unordered_set<int> seen;
    for (int id : got) {
        if (seen.count(id)) return false;
        seen.insert(id);
    }

    // got 必须整体降序 (按各自最高分)
    for (int i = 1; i < (int)got.size(); ++i)
        if (bestScore(got[i-1]) < bestScore(got[i]) - EPS) return false;

    // got 的分数多重集 == want 的分数多重集
    vector<double> gs, ws;
    for (int id : got)  gs.push_back(bestScore(id));
    for (int id : want) ws.push_back(bestScore(id));
    sort(gs.begin(), gs.end());
    sort(ws.begin(), ws.end());
    for (int i = 0; i < (int)gs.size(); ++i)
        if (fabs(gs[i]-ws[i]) > EPS) return false;

    return true;
}

void runCase(const string& name,
             const vector<vector<Result>>& streams, int N) {
    auto got  = mergeTopN(streams, N);
    auto want = bruteForce(streams, N);
    bool ok = validate(got, want, streams);
    cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name
         << "  got.size=" << got.size() << " want.size=" << want.size() << "\n";
    if (!ok) {
        cout << "    got : "; for (int x : got)  cout << x << " "; cout << "\n";
        cout << "    want: "; for (int x : want) cout << x << " "; cout << "\n";
    }
}

int main() {
    // --- 退化 ---
    runCase("empty streams",        {}, 5);
    runCase("all empty lists",      {{},{},{}}, 5);
    runCase("N=0",                  {{{1,0.9},{2,0.5}}}, 0);
    runCase("single stream",        {{{1,0.9},{2,0.7},{3,0.5}}}, 2);
    runCase("N larger than total",  {{{1,0.9}},{{2,0.5}}}, 10);

    // --- 基本多路 ---
    runCase("two streams no dup",
        {{{1,0.9},{3,0.6}}, {{2,0.8},{4,0.4}}}, 3);

    // --- 去重: 同 doc 多路出现, 取最高分 ---
    // doc 1 在两路: 0.9 和 0.5 -> 取 0.9
    runCase("dup take max",
        {{{1,0.9},{2,0.3}}, {{1,0.5},{3,0.7}}}, 3);

    // --- 去重 + 顺序: 确认第一次弹出即最高分 ---
    runCase("dup ordering",
        {{{5,0.95},{1,0.80}}, {{1,0.99},{6,0.10}}}, 2);
    // 注意: doc1 最高分应是 0.99 (来自第二路), 不是第一路先遇到的 0.80

    // --- score 并列 ---
    runCase("tie scores",
        {{{1,0.5},{2,0.5}}, {{3,0.5},{4,0.5}}}, 2);

    // --- 三路, 交错 ---
    runCase("three interleaved",
        {{{1,0.90},{4,0.40}}, {{2,0.80},{5,0.30}}, {{3,0.60},{6,0.20}}}, 4);

    // --- 随机对拍 ---
    std::mt19937 rng(123);
    std::uniform_int_distribution<int> kDist(0, 6);      // 路数
    std::uniform_int_distribution<int> lenDist(0, 12);   // 每路长度
    std::uniform_int_distribution<int> idDist(1, 20);    // doc_id 池 (小, 制造重复)
    std::uniform_int_distribution<int> nDist(0, 25);     // N
    std::uniform_real_distribution<double> scDist(0.0, 1.0);

    int trials = 3000, passed = 0;
    for (int t = 0; t < trials; ++t) {
        int k = kDist(rng);
        vector<vector<Result>> streams(k);
        for (int i = 0; i < k; ++i) {
            int len = lenDist(rng);
            vector<Result> s;
            for (int j = 0; j < len; ++j)
                s.push_back({idDist(rng), round(scDist(rng)*100)/100.0});
            // 每路按 score 降序排 (题目前提)
            sort(s.begin(), s.end(), [](const Result&a,const Result&b){return a.score>b.score;});
            streams[i] = s;
        }
        int N = nDist(rng);
        auto got  = mergeTopN(streams, N);
        auto want = bruteForce(streams, N);
        if (validate(got, want, streams)) ++passed;
        else {
            cout << "[FAIL] random trial " << t << " k=" << k << " N=" << N << "\n";
            cout << "    got : "; for (int x:got) cout<<x<<" "; cout<<"\n";
            cout << "    want: "; for (int x:want) cout<<x<<" "; cout<<"\n";
        }
    }
    cout << "random oracle: " << passed << "/" << trials << " passed\n";

    return 0;
}