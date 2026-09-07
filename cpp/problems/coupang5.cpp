// Coupang Staff loop - count how many connected components of an undirected
// graph are trees (connected and acyclic).
//
// Build: g++ -std=c++17 -O0 -g treecomponents.cpp && ./a.out

#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <set>
#include <utility>
#include <functional>
#include <algorithm>

using namespace std;

// ---------------------------------------------------------------- your code

int countTreeComponents(int n, const vector<pair<int, int>>& edges) {
    
    vector<bool> vis(n, false);
    vector<vector<int>> graph(n, vector<int>());
    for (auto [a, b]: edges) {
        graph[a].push_back(b);
        graph[b].push_back(a);
    }
    int node_count =0;
    int edge_count =0;
    //1-2-3-1
    std::function<void(int)> dfs =[&](int r) {
        node_count ++;
        for (int i=0;i<graph[r].size();i++) {
            int next = graph[r][i];
            edge_count++;
            if (vis[next] ==true) {
                continue;
            }        
            
            vis[next] =true;
            dfs(next);
        }
    };
    int ret =0;
    for (int i=0;i<n;i++) {
        node_count =0;
        edge_count =0;
        if (vis[i]) {
            continue;
        }
        vis[i] =true;
        dfs(i);
        if (node_count-1 ==edge_count/2) {
            ret ++;
        }
    }
    return ret;
}

// ---------------------------------------------------------------- test harness

static int failures = 0;

static void expectEq(int n, const vector<pair<int,int>>& edges, int want, const char* tag) {
    int got = countTreeComponents(n, edges);
    if (got != want) {
        cout << "FAIL [" << tag << "]  n=" << n << " edges={";
        for (auto& e : edges) cout << "(" << e.first << "," << e.second << ")";
        cout << "}  got " << got << ", want " << want << "\n";
        ++failures;
    }
}

// reference: union-find over the multiset of edges, then per component compare
// vertex count against the number of DISTINCT edges (self-loops counted once,
// which is enough to break the k-1 equality).
static int brute(int n, const vector<pair<int,int>>& edges) {
    vector<int> p(n);
    for (int i = 0; i < n; ++i) p[i] = i;
    function<int(int)> find = [&](int x) { while (p[x] != x) { p[x] = p[p[x]]; x = p[x]; } return x; };
    set<pair<int,int>> uniq;
    for (auto& e : edges) {
        int a = e.first, b = e.second;
        uniq.insert({min(a,b), max(a,b)});
        int ra = find(a), rb = find(b);
        if (ra != rb) p[ra] = rb;
    }
    vector<int> vcount(n, 0), ecount(n, 0);
    for (int i = 0; i < n; ++i) vcount[find(i)]++;
    for (auto& e : uniq) ecount[find(e.first)]++;
    int res = 0;
    for (int i = 0; i < n; ++i) if (find(i) == i && ecount[i] == vcount[i] - 1) ++res;
    return res;
}

int main() {
    // --- isolated vertices are single-node trees
    expectEq(3, {}, 3, "three isolated");
    expectEq(1, {}, 1, "single vertex");
    expectEq(0, {}, 0, "empty graph");

    // --- one simple tree
    expectEq(3, {{0,1},{1,2}}, 1, "path of three");
    expectEq(4, {{0,1},{0,2},{0,3}}, 1, "star");

    // --- one cycle, not a tree
    expectEq(3, {{0,1},{1,2},{2,0}}, 0, "triangle");
    expectEq(4, {{0,1},{1,2},{2,3},{3,0}}, 0, "square");

    // --- mixed: one tree component + one cyclic component
    expectEq(6, {{0,1},{1,2},{3,4},{4,5},{5,3}}, 1, "tree + triangle");

    // --- tree component + isolated vertex
    expectEq(4, {{0,1},{1,2}}, 2, "path + isolated");

    // --- self loop is a cycle
    expectEq(1, {{0,0}}, 0, "self loop alone");
    expectEq(2, {{0,0},{0,1}}, 0, "self loop attached");
    expectEq(3, {{0,0},{1,2}}, 1, "self loop + separate edge");

    // --- parallel edge is a cycle
    expectEq(2, {{0,1},{0,1}}, 0, "parallel edge");
    expectEq(2, {{0,1},{1,0}}, 0, "parallel edge reversed");
    expectEq(4, {{0,1},{1,0},{2,3}}, 1, "parallel + separate tree");

    // --- larger mixed graph
    expectEq(9, {{0,1},{1,2},{2,0},   // triangle
                 {3,4},{4,5},          // path
                 {6,7}},               // edge
              2, "triangle + path + edge + isolated 8");

    // --- randomized differential test
    srand(20260902);
    for (int trial = 0; trial < 3000 && failures < 5; ++trial) {
        int n = 1 + rand() % 8;
        int m = rand() % 10;
        vector<pair<int,int>> es;
        for (int i = 0; i < m; ++i) es.push_back({rand() % n, rand() % n});
        int got = countTreeComponents(n, es);
        int want = brute(n, es);
        if (got != want) {
            cout << "RANDOM FAIL  n=" << n << " edges={";
            for (auto& e : es) cout << "(" << e.first << "," << e.second << ")";
            cout << "}  got " << got << ", want " << want << "\n";
            ++failures;
        }
    }

    cout << (failures ? "FAILURES: " : "all passed, failures: ") << failures << "\n";
    return failures != 0;
}