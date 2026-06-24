#include <vector>
#include <iostream>
#include <queue>
#include <unordered_set>
#include <algorithm>
#include <random>
using namespace std;

// edges[k] = {a, b} 表示 a 必须在 b 之前加载 (有向边 a -> b)
// 返回一个合法加载顺序; 若有环 (依赖矛盾) 返回空 vector
std::vector<int> loadOrder(int n, const std::vector<std::vector<int>>& edges) {
    if (n<=0) {
        return {};
    }
    vector<int> in_degree(n, 0);
    vector<vector<int>> graph(n, vector<int>());
    for (auto edge: edges) {
        int a =edge[0];
        int b =edge[1];
        in_degree[b]++;
        graph[a].push_back(b);
    }
    queue<int> q;
    for(size_t i=0;i<in_degree.size();i++) {
        if (in_degree[i] ==0) {
            q.push(i);
        }
    }
    vector<int> ret;
    while (!q.empty()) {
        int t =q.front();
        q.pop();
        ret.push_back(t);
        for (auto b : graph[t]) {
            --in_degree[b];
            if (in_degree[b] ==0) {
                q.push(b);
            } 
        }
    }
    if (ret.size()==n) {
        return ret;
    }
    return {};
}










// ==== 校验器: 检查返回的顺序是否是一个合法拓扑序 ====
// 返回 true 当且仅当:
//   - 若图有环: 返回的 order 必须为空
//   - 若图无环: order 必须是 0..n-1 的一个排列, 且满足所有边的先后约束
bool hasCycle(int n, const vector<vector<int>>& edges) {
    vector<vector<int>> adj(n);
    vector<int> indeg(n, 0);
    for (auto& e : edges) { adj[e[0]].push_back(e[1]); indeg[e[1]]++; }
    queue<int> q;
    for (int i = 0; i < n; ++i) if (indeg[i] == 0) q.push(i);
    int seen = 0;
    while (!q.empty()) {
        int u = q.front(); q.pop(); ++seen;
        for (int v : adj[u]) if (--indeg[v] == 0) q.push(v);
    }
    return seen != n;
}

bool validate(int n, const vector<vector<int>>& edges, const vector<int>& order) {
    bool cyc = hasCycle(n, edges);
    if (cyc) return order.empty();

    // 无环: 必须是 0..n-1 的排列
    if ((int)order.size() != n) return false;
    vector<int> seen(n, 0);
    for (int x : order) {
        if (x < 0 || x >= n || seen[x]) return false;
        seen[x] = 1;
    }
    // 每条边 a->b: a 必须排在 b 之前
    vector<int> pos(n);
    for (int i = 0; i < n; ++i) pos[order[i]] = i;
    for (auto& e : edges) if (pos[e[0]] >= pos[e[1]]) return false;
    return true;
}

void runCase(const string& name, int n, const vector<vector<int>>& edges) {
    auto order = loadOrder(n, edges);
    bool ok = validate(n, edges, order);
    cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name
         << "  (n=" << n << ", cyclic=" << (hasCycle(n,edges) ? "yes" : "no")
         << ", order.size=" << order.size() << ")\n";
    if (!ok) {
        cout << "    order: "; for (int x : order) cout << x << " "; cout << "\n";
    }
}

int main() {
    // --- 退化 ---
    runCase("n=0",                 0, {});
    runCase("n=1 no edges",        1, {});
    runCase("two nodes no edges",  2, {});

    // --- 简单链 ---
    runCase("simple chain",        3, {{0,1},{1,2}});           // 0->1->2

    // --- DAG 多分支 ---
    runCase("diamond",             4, {{0,1},{0,2},{1,3},{2,3}}); // 0 ->1,2 ->3

    // --- 自环 (一定有环) ---
    runCase("self loop",           1, {{0,0}});

    // --- 二元环 ---
    runCase("two cycle",           2, {{0,1},{1,0}});

    // --- 三元环 ---
    runCase("three cycle",         3, {{0,1},{1,2},{2,0}});

    // --- 部分有环 (无环部分 + 一个环) ---
    runCase("partial cycle",       5, {{0,1},{1,2},{2,1},{3,4}}); // 1<->2 成环

    // --- 多个独立连通分量, 无环 ---
    runCase("two components",      4, {{0,1},{2,3}});

    // --- 较宽的 DAG ---
    runCase("wide dag",            6, {{0,1},{0,2},{0,3},{1,4},{2,4},{3,5}});

    // --- 重复边 (无环) ---
    runCase("duplicate edge",      3, {{0,1},{0,1},{1,2}});

    // --- 随机对拍 ---
    std::mt19937 rng(99);
    int trials = 5000, passed = 0;
    for (int t = 0; t < trials; ++t) {
        int n = rng() % 8;                 // 0..7 个节点
        int maxEdges = rng() % 12;
        vector<vector<int>> edges;
        for (int e = 0; e < maxEdges; ++e) {
            if (n == 0) break;
            int a = rng() % n, b = rng() % n;
            edges.push_back({a, b});       // 允许自环和重复边, 制造各种环
        }
        auto order = loadOrder(n, edges);
        if (validate(n, edges, order)) ++passed;
        else {
            cout << "[FAIL] random trial " << t << " n=" << n
                 << " cyclic=" << (hasCycle(n,edges)?"yes":"no")
                 << " order.size=" << order.size() << "\n";
            cout << "    edges: ";
            for (auto& e : edges) cout << "(" << e[0] << "," << e[1] << ") ";
            cout << "\n    order: ";
            for (int x : order) cout << x << " ";
            cout << "\n";
        }
    }
    cout << "random oracle: " << passed << "/" << trials << " passed\n";

    return 0;
}