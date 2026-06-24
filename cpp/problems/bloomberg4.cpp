#include <iostream>
#include <vector>
#include <algorithm>
#include <functional>
#include <stack>
using namespace std;

// ============================================================
//  TODO: 你来实现
//  输入: n 个节点 (0..n-1), 邻接表 g (g[u] = u 直接指向的节点列表)
//  输出: 所有 SCC。每个 SCC 是一个 vector<int>(分量内的节点)。
//        分量之间顺序不限; 分量内部节点顺序不限(测试会各自排序后比较)。
// ============================================================
vector<vector<int>> tarjanSCC(int n, const vector<vector<int>> &g)
{
    // 你的代码
    vector<int> low(n, INT_MAX);
    vector<int> dfn(n, -1);

    vector<bool> vis(n, false);
    vector<bool> processed(n, false);
    stack<int> st;
    vector<vector<int>> ret;
    
    function<void(int, int &)> dfs = [&](int r, int &cnt)
    {
        dfn[r] = cnt++;
        low[r] =  dfn[r];
        st.push(r);
        for (int i = 0; i < g[r].size(); i++)
        {
            int j = g[r][i];
            if (!vis[j])
            {
                vis[j] = true;
                dfs(j, cnt);
            }
            if (!processed[low[j]]) {
                low[r] = min(low[r], low[j]);
            }
        }
        if (low[r] == dfn[r])
        {
            // construct ssr
            vector<int> scc;
            while (!st.empty()) {
                int x = st.top();
                if (dfn[x] >= dfn[r]) {
                    processed[dfn[x]] =true;
                    scc.push_back(x);
                }
                else {
                    break;
                }
                st.pop();
            }
            ret.push_back(scc);
        }
    };
    int cnt =0;
    for (int i=0;i<n;i++) {
        if (!vis[i]) {
            vis[i] =true;
            dfs(i, cnt);
        }
    }
    return ret;
}

// ----------------- 下面是测试,不用动 -----------------

// 把结果规范化: 每个分量内部排序, 再按分量首元素排序 —— 便于和期望比较
static vector<vector<int>> normalize(vector<vector<int>> comps)
{
    for (auto &c : comps)
        sort(c.begin(), c.end());
    sort(comps.begin(), comps.end());
    return comps;
}

static void check(const string &name,
                  vector<vector<int>> got,
                  vector<vector<int>> exp)
{
    got = normalize(move(got));
    exp = normalize(move(exp));
    auto dump = [](const vector<vector<int>> &cs)
    {
        string s = "{";
        for (auto &c : cs)
        {
            s += "[";
            for (int x : c)
            {
                s += to_string(x);
                s += " ";
            }
            s += "]";
        }
        return s + "}";
    };
    cout << name << "  got=" << dump(got)
         << "  exp=" << dump(exp)
         << "  " << (got == exp ? "PASS" : "FAIL") << "\n";
}

int main()
{
    // T1: 单个 3-环  0->1->2->0   => 一个 SCC {0,1,2}
    check("single_cycle",
          tarjanSCC(3, {{1}, {2}, {0}}),
          {{0, 1, 2}});

    // T2: 无环链  0->1->2  => 三个单点 SCC
    check("chain_no_cycle",
          tarjanSCC(3, {{1}, {2}, {}}),
          {{0}, {1}, {2}});

    // T3: 一个环挂一条尾巴  0->1->2->0, 2->3  => {0,1,2} 和 {3}
    check("cycle_with_tail",
          tarjanSCC(4, {{1}, {2}, {0, 3}, {}}),
          {{0, 1, 2}, {3}});

    // T4: 两个环经由共享方向相连(经典 merge 场景)
    //  0->1, 1->2, 2->0   (环A {0,1,2})
    //  2->3, 3->4, 4->3   (环B {3,4})
    //  => {0,1,2} 和 {3,4}
    check("two_components",
          tarjanSCC(5, {{1}, {2}, {0, 3}, {4}, {3}}),
          {{0, 1, 2}, {3, 4}});

    // T5: 嵌套式 —— 两个三元环共享一个点, 整体强连通
    //  0->1->2->0  和  0->3->4->0   共享 0  => 全图一个 SCC {0,1,2,3,4}
    check("shared_vertex_one_scc",
          tarjanSCC(5, {{1, 3}, {2}, {0}, {4}, {0}}),
          {{0, 1, 2, 3, 4}});

    // T6: 自环 + 孤立点  0->0, 1 孤立  => {0} 和 {1}
    check("self_loop_and_isolated",
          tarjanSCC(2, {{0}, {}}),
          {{0}, {1}});

    // T7: 较大混合
    //  SCC: {0,1,2} (0->1->2->1? 不) —— 明确给:
    //  0->1,1->2,2->0  => {0,1,2}
    //  3->4,4->5,5->3  => {3,4,5}
    //  2->3 (单向桥, 不合并)
    //  6 单点, 5->6
    check("larger_mixed",
          tarjanSCC(7, {{1}, {2}, {0, 3}, {4}, {5}, {3, 6}, {}}),
          {{0, 1, 2}, {3, 4, 5}, {6}});

    return 0;
}