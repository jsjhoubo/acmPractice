// ============================================================
// Agent Memory Infra — 参考实现合集 (C++17)
// 题1 LFU | 题2 版本化KV | 题3 去重(并查集+倒排) | 题4 LRU-K
// 题5 Token预算背包(含树形) | 题6 Sharded Counter
// 每题独立 namespace,底部 main() 全部测试
// ============================================================
#include <bits/stdc++.h>
using namespace std;

// ============================================================
// 题1: LFU Cache — 频次桶 + 桶内 LRU,全 O(1)
// 核心不变量: minFreq 始终指向最小非空频次桶
// ============================================================
namespace p1 {
class LFUCache {
    int cap, minFreq = 0;
    // freq -> 该频次的 key 列表(front = 最新访问)
    unordered_map<int, list<int>> buckets;
    struct Node { int value, freq; list<int>::iterator it; };
    unordered_map<int, Node> table;

    // 访问 key: 从旧频次桶摘出,挂到 freq+1 桶头
    void touch(int key) {
        auto& nd = table[key];
        buckets[nd.freq].erase(nd.it);
        if (buckets[nd.freq].empty()) {
            buckets.erase(nd.freq);
            if (minFreq == nd.freq) minFreq++;   // 只可能 +1:该 key 就在原 minFreq 桶
        }
        nd.freq++;
        buckets[nd.freq].push_front(key);
        nd.it = buckets[nd.freq].begin();
    }
public:
    LFUCache(int capacity) : cap(capacity) {}

    int get(int key) {
        auto it = table.find(key);
        if (it == table.end()) return -1;
        touch(key);
        return it->second.value;
    }

    void put(int key, int value) {
        if (cap == 0) return;
        auto it = table.find(key);
        if (it != table.end()) {            // 覆盖 = 更新值 + 一次访问
            it->second.value = value;
            touch(key);
            return;
        }
        if ((int)table.size() == cap) {     // 驱逐: minFreq 桶的尾部(桶内 LRU)
            int victim = buckets[minFreq].back();
            buckets[minFreq].pop_back();
            if (buckets[minFreq].empty()) buckets.erase(minFreq);
            table.erase(victim);
        }
        buckets[1].push_front(key);         // 新 key 频次 = 1
        table[key] = {value, 1, buckets[1].begin()};
        minFreq = 1;                        // 新 key 必然使 minFreq 归 1
    }
};
}

// ============================================================
// 题2: 版本化 KV — set 单调,get 任意时间点读
// key -> 按 timestamp 升序的 (ts, value) 数组,get 二分
// set O(1) 均摊, get O(log n)
// ============================================================
namespace p2 {
class VersionedKV {
    unordered_map<string, vector<pair<long long, string>>> hist;
public:
    void set(long long ts, const string& key, const string& value) {
        hist[key].emplace_back(ts, value);  // ts 单调保证有序
    }
    string get(long long ts, const string& key) {
        auto it = hist.find(key);
        if (it == hist.end()) return "";
        auto& v = it->second;
        // 找最后一个 ts_i <= ts: upper_bound 后退一格
        auto pos = upper_bound(v.begin(), v.end(), ts,
            [](long long t, const pair<long long, string>& p){ return t < p.first; });
        if (pos == v.begin()) return "";    // 全部晚于查询时间
        return prev(pos)->second;
    }
};
}

// ============================================================
// 题3: 记忆去重 — 倒排索引生成候选对 + Jaccard 过滤 + 并查集
// 只有共享 token 的 pair 才可能 Jaccard>0,避免全 n^2
// ============================================================
namespace p3 {
struct DSU {
    vector<int> fa;
    DSU(int n) : fa(n) { iota(fa.begin(), fa.end(), 0); }
    int find(int x) { return fa[x] == x ? x : fa[x] = find(fa[x]); }
    void unite(int a, int b) {
        a = find(a); b = find(b);
        if (a != b) fa[max(a,b)] = min(a,b);  // 小 id 做根,天然满足"保留最小 id"
    }
};

// entries[i] = 该记忆的 token 集合(已去重)。返回每组保留的最小 id 及其组员。
vector<vector<int>> dedup(const vector<set<string>>& entries, double threshold) {
    int n = entries.size();
    unordered_map<string, vector<int>> inv;          // token -> 含它的条目
    for (int i = 0; i < n; i++)
        for (auto& t : entries[i]) inv[t].push_back(i);

    set<pair<int,int>> candidates;                   // 共享≥1 token 的 pair
    for (auto& [tok, ids] : inv)
        for (size_t a = 0; a + 1 < ids.size(); a++)
            for (size_t b = a + 1; b < ids.size(); b++)
                candidates.insert({ids[a], ids[b]});

    DSU dsu(n);
    for (auto& [i, j] : candidates) {
        // Jaccard = |A∩B| / |A∪B|
        size_t inter = 0;
        const auto& A = entries[i], & B = entries[j];
        const auto& small_ = A.size() < B.size() ? A : B;
        const auto& big_   = A.size() < B.size() ? B : A;
        for (auto& t : small_) inter += big_.count(t);
        double jac = (double)inter / (A.size() + B.size() - inter);
        if (jac >= threshold) dsu.unite(i, j);
    }
    map<int, vector<int>> groups;                    // 根(=组内最小 id) -> 组员
    for (int i = 0; i < n; i++) groups[dsu.find(i)].push_back(i);
    vector<vector<int>> res;
    for (auto& [root, mem] : groups) res.push_back(mem);
    return res;
}
}

// ============================================================
// 题4: LRU-K — 试用队列(FIFO) + 正式队列(LRU)
// 访问 < K 次: 试用队列,优先驱逐(FIFO)
// 访问 >= K 次: 晋升正式队列,按 LRU;正式满则试用先腾
// ============================================================
namespace p4 {
class LRUK {
    int cap, K;
    struct Node { int value, cnt; bool formal; list<int>::iterator it; };
    unordered_map<int, Node> table;
    list<int> probation;   // FIFO: front=最老 → 驱逐端
    list<int> formal;      // LRU:  front=最新, back=驱逐端

    void evictOne() {                       // 试用优先
        if (!probation.empty()) {
            table.erase(probation.front());
            probation.pop_front();
        } else {
            table.erase(formal.back());
            formal.pop_back();
        }
    }
    void access(int key) {                  // 计数 + 队列迁移
        auto& nd = table[key];
        nd.cnt++;
        if (nd.formal) {                    // 已在正式队列: 移到头(LRU touch)
            formal.erase(nd.it);
            formal.push_front(key);
            nd.it = formal.begin();
        } else if (nd.cnt >= K) {           // 晋升
            probation.erase(nd.it);
            nd.formal = true;
            formal.push_front(key);
            nd.it = formal.begin();
        }                                    // 未达 K: 留在试用队列原位(FIFO 不动)
    }
public:
    LRUK(int capacity, int k) : cap(capacity), K(k) {}
    int get(int key) {
        auto it = table.find(key);
        if (it == table.end()) return -1;
        access(key);
        return it->second.value;
    }
    void put(int key, int value) {
        auto it = table.find(key);
        if (it != table.end()) { it->second.value = value; access(key); return; }
        if ((int)table.size() == cap) evictOne();
        probation.push_back(key);           // 新 key 从试用队列尾进(FIFO 入口)
        table[key] = {value, 1, false, prev(probation.end())};
        if (K <= 1) access(key), table[key].cnt = 1;  // K=1 退化为纯 LRU: 立即晋升
    }
};
}

// ============================================================
// 题5: Token 预算下的上下文组装
// 5a 无依赖: 0/1 背包 O(nB)
// 5b 依赖成森林: 树形背包(dfs 序 + 子树跳转)O(nB)
// ============================================================
namespace p5 {
// 5a: 0/1 背包
long long knapsack(const vector<int>& tokens, const vector<long long>& score, int B) {
    vector<long long> dp(B + 1, 0);
    for (size_t i = 0; i < tokens.size(); i++)
        for (int b = B; b >= tokens[i]; b--)
            dp[b] = max(dp[b], dp[b - tokens[i]] + score[i]);
    return dp[B];
}

// 5b: 树形依赖背包 —— 选 i 必须选 parent[i](parent=-1 为根,挂到虚拟根 0)
// 经典 dfs 序 DP: dp[i][b] = 考虑 dfs 序第 i 个节点起的最优
//   选它:   dp[i+1][b - w]  (进入子树)
//   不选它: dp[skip(i)][b]  (整棵子树跳过 —— 依赖约束的体现)
long long treeKnapsack(const vector<int>& parent, const vector<int>& tokens,
                       const vector<long long>& score, int B) {
    int n = parent.size();
    vector<vector<int>> ch(n + 1);              // 节点 1..n, 0 = 虚拟根
    for (int i = 0; i < n; i++) ch[parent[i] + 1].push_back(i + 1);

    vector<int> order, skipTo(n + 2);           // dfs 序及每个位置的子树末端+1
    // 迭代 dfs 生成后序……这里用递归简洁版
    function<void(int)> dfs = [&](int u) {
        int pos = order.size();
        if (u != 0) order.push_back(u);
        for (int v : ch[u]) dfs(v);
        if (u != 0) skipTo[pos] = order.size(); // 跳过 u 的整棵子树后到达的位置
    };
    dfs(0);
    int m = order.size();
    // dp[i][b]: 从 dfs 序位置 i 起,预算 b 的最大得分(倒序填)
    vector<vector<long long>> dp(m + 1, vector<long long>(B + 1, 0));
    for (int i = m - 1; i >= 0; i--) {
        int u = order[i] - 1;                   // 还原 0-based 原编号
        for (int b = 0; b <= B; b++) {
            long long best = dp[skipTo[i]][b];  // 不选: 跳过整棵子树
            if (b >= tokens[u])
                best = max(best, dp[i + 1][b - tokens[u]] + score[u]);  // 选: 进子树
            dp[i][b] = best;
        }
    }
    return dp[0][B];
}
}

// ============================================================
// 题6: Sharded Counter — increment 低竞争,snapshot 一致聚合
// 每 shard 一把锁 + 局部 map;snapshot 逐 shard 加锁合并
// (无锁版思路: 每线程 thread_local 计数 + epoch 回收,面试口述即可)
// ============================================================
namespace p6 {
class ShardedCounter {
    static const int S = 16;
    struct alignas(64) Shard {                  // 64B 对齐防 false sharing
        mutex mu;
        unordered_map<string, long long> cnt;
    };
    array<Shard, S> shards;
    size_t idx(const string& k) const { return hash<string>{}(k) % S; }
public:
    void increment(const string& key, long long delta = 1) {
        auto& sh = shards[idx(key)];
        lock_guard<mutex> lg(sh.mu);            // 只竞争 1/S 的锁
        sh.cnt[key] += delta;
    }
    // 语义: 非全局原子快照,而是"每个 shard 各自某时刻的一致值"的合并。
    // 若需全局一致: 按序锁全部 shard 再读(锁序固定防死锁),或 epoch 方案。
    unordered_map<string, long long> snapshot() {
        unordered_map<string, long long> out;
        for (auto& sh : shards) {
            lock_guard<mutex> lg(sh.mu);
            for (auto& [k, v] : sh.cnt) out[k] += v;
        }
        return out;
    }
};
}

// ============================================================
// 测试
// ============================================================
int main() {
    // ---- 题1 LFU (LC460 官方样例 + 覆盖/平频次) ----
    {
        p1::LFUCache c(2);
        c.put(1,1); c.put(2,2);
        assert(c.get(1)==1);        // freq: 1->2
        c.put(3,3);                 // 驱逐 2 (freq 最低)
        assert(c.get(2)==-1);
        assert(c.get(3)==3);
        c.put(4,4);                 // 1,3 频次同=2, 驱逐更旧的 1
        assert(c.get(1)==-1);
        assert(c.get(3)==3); assert(c.get(4)==4);
        p1::LFUCache z(0); z.put(1,1); assert(z.get(1)==-1);  // cap=0
    }
    // ---- 题2 版本化 KV ----
    {
        p2::VersionedKV kv;
        kv.set(1,"a","x"); kv.set(5,"a","y"); kv.set(9,"a","z");
        assert(kv.get(0,"a")=="");
        assert(kv.get(1,"a")=="x");
        assert(kv.get(4,"a")=="x");
        assert(kv.get(5,"a")=="y");
        assert(kv.get(100,"a")=="z");
        assert(kv.get(3,"b")=="");
    }
    // ---- 题3 去重 ----
    {
        // e0={a,b,c}, e1={a,b,d}, e2={x,y}, e3={a,b,c,d} 阈值 0.5
        // J(0,1)=2/4=0.5 ✓  J(0,3)=3/4 ✓  J(1,3)=3/4 ✓  → {0,1,3} 一组, {2} 一组
        vector<set<string>> es = {{"a","b","c"},{"a","b","d"},{"x","y"},{"a","b","c","d"}};
        auto g = p3::dedup(es, 0.5);
        assert(g.size()==2);
        assert((g[0]==vector<int>{0,1,3}));
        assert((g[1]==vector<int>{2}));
    }
    // ---- 题4 LRU-K (K=2) ----
    {
        p4::LRUK c(2,2);
        c.put(1,10); c.put(2,20);          // 都在试用队列, FIFO: [1,2]
        c.put(3,30);                       // 满: 驱逐试用队首 1
        assert(c.get(1)==-1);
        assert(c.get(2)==20);              // 2 达到 2 次 → 晋升正式
        c.put(4,40);                       // 驱逐试用队首 3 (正式的 2 受保护)
        assert(c.get(3)==-1);
        assert(c.get(2)==20);
        assert(c.get(4)==40);              // 4 也晋升
        c.put(5,50);                       // 试用空 → 驱逐正式 LRU 端 = 2
        assert(c.get(2)==-1);
        assert(c.get(4)==40);
    }
    // ---- 题5 背包 ----
    {
        // 5a: 经典
        // 选 items 1,2: tokens 4+5=9 <= 9, score 5+6=11
        assert(p5::knapsack({3,4,5},{4,5,6},9) == 11);
        assert(p5::knapsack({2,2,2},{3,3,3},4) == 6);
        // 5b: 依赖链 0<-1<-2 (选 2 必须选 1 必须选 0)
        // tokens {1,1,1} score {1,1,10} B=2: 只能选 {0,1} score=2 (够不到 2)
        // B=3: 选全部 score=12
        assert(p5::treeKnapsack({-1,0,1},{1,1,1},{1,1,10},2) == 2);
        assert(p5::treeKnapsack({-1,0,1},{1,1,1},{1,1,10},3) == 12);
        // 森林: 两棵独立树
        assert(p5::treeKnapsack({-1,-1},{2,2},{3,5},2) == 5); // 只装得下一个,选 score 大的
    }
    // ---- 题6 Sharded Counter ----
    {
        p6::ShardedCounter sc;
        vector<thread> ts;
        for (int t = 0; t < 8; t++)
            ts.emplace_back([&sc]{ for (int i = 0; i < 10000; i++) sc.increment("hits"); });
        for (auto& t : ts) t.join();
        auto snap = sc.snapshot();
        assert(snap["hits"] == 80000);
    }
    cout << "All 6 problem sets passed.\n";
    return 0;
}