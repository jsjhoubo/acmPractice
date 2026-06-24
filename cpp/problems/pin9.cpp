#include <iostream>
#include <vector>
#include <string>
#include <functional>
#include <set>
using namespace std;

// ============================================================
// Pinterest 灵感画板生成
//   R×C 网格, '.' = 空格待填, 其他字符 = 预填类别(固定不可改)
//   可用类别: 'A' .. 'A'+k-1
//   约束: 任意上下左右相邻格子类别不能相同
//
//   Q(a) canFill        : 是否存在合法填法 -> bool
//   Q(b) fillAny        : 返回任意一种合法填法; 无解返回空 vector
//   Q(c) fillLexSmallest: 返回字典序最小填法(行优先读成字符串最小)
// ============================================================

class BoardFiller
{
public:
    BoardFiller(int k) : _k(k) {}

    // Q(a)
    bool canFill(vector<vector<char>> &grid)
    {
        // 你写: 回溯
        int n = grid.size();
        if (n == 0)
            return false;
        int m = grid[0].size();
        if (m == 0)
            return false;
        auto valid = [&](int r, int c) -> bool
        {
            return r >= 0 && r < n &&
                   c >= 0 && c < m;
        };
        int dir[2][2] = {{0, -1}, {-1, 0}};
        bool succ = false;
        function<void(int, int)> dfs = [&](int r, int c)
        {
            if (r == n)
            {
                succ = true;
                return;
            }
            set<char> kind;
            for (int i = 0; i < 2; i++)
            {
                int r1 = r + dir[i][0];
                int c1 = c + dir[i][1];
                if (!valid(r1, c1))
                {
                    continue;
                }
                kind.insert(grid[r1][c1]);
            }
            if (grid[r][c] == '.')
            {
                for (int k = 0; k < _k; k++)
                {
                    char ch = k + 'A';
                    if (kind.find(ch) == kind.end())
                    {
                        grid[r][c] = ch;
                        if (c + 1 < m)
                        {
                            dfs(r, c + 1);
                        }
                        else
                        {
                            if (r < n)
                            {
                                dfs(r + 1, 0);
                            }
                        }
                        if (succ)
                        {
                            return;
                        }
                        grid[r][c] = '.';
                    }
                }
            }
            else
            {
                if (kind.find(grid[r][c]) != kind.end())
                {
                    return;
                }
                if (c + 1 < m)
                {
                    dfs(r, c + 1);
                }
                else
                {
                    if (r < n)
                    {
                        dfs(r + 1, 0);
                    }
                }
            }
        };
        dfs(0, 0);
        return succ;
    }

    // Q(b): 返回填好的 grid; 无解返回 {}
    vector<vector<char>> fillAny(vector<vector<char>> grid)
    {
        bool fill = canFill(grid);
        if (fill)
            return grid;
        return {};
    }

    // Q(c): 字典序最小
    vector<vector<char>> fillLexSmallest(vector<vector<char>> grid)
    {
        // 你写: 每个空格按 'A'->'A'+k-1 顺序试, 第一个能走通的就选
        bool fill = canFill(grid);
        if (fill)
            return grid;
        return {};
    }

private:
    int _k;
    // 你可能需要的 helper: 检查在 (r,c) 放 ch 是否和 4 邻居冲突
    // bool ok(grid, r, c, ch) { ... }
    // 回溯主体: dfs(grid, idx) 按行优先填第 idx 个 '.'
};

// ============ TEST HARNESS ============
void printGrid(const vector<vector<char>> &g)
{
    cout << "{ ";
    for (auto &row : g)
    {
        for (char c : row)
            cout << c;
        cout << " ";
    }
    cout << "}";
}

// 校验一个填好的 grid 是否合法(无 '.', 相邻不撞, 且与原预填一致)
bool validate(const vector<vector<char>> &orig, const vector<vector<char>> &g, int k)
{
    if (g.empty())
        return false;
    int R = g.size(), C = g[0].size();
    int dr[4] = {-1, 1, 0, 0}, dc[4] = {0, 0, -1, 1};
    for (int r = 0; r < R; r++)
        for (int c = 0; c < C; c++)
        {
            char ch = g[r][c];
            if (ch < 'A' || ch >= 'A' + k)
                return false; // 越界类别
            if (orig[r][c] != '.' && orig[r][c] != ch)
                return false; // 改了预填
            for (int d = 0; d < 4; d++)
            {
                int nr = r + dr[d], nc = c + dc[d];
                if (nr >= 0 && nr < R && nc >= 0 && nc < C && g[nr][nc] == ch)
                    return false; // 相邻撞
            }
        }
    return true;
}

void runA(const string &name, BoardFiller &f, vector<vector<char>> g, bool expected)
{
    bool got = f.canFill(g);
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

void runB(const string &name, BoardFiller &f, vector<vector<char>> g, int k, bool expectSolvable)
{
    auto got = f.fillAny(g);
    bool ok;
    if (!expectSolvable)
        ok = got.empty();
    else
        ok = validate(g, got, k);
    cout << name << ": " << (ok ? "PASS" : "*** FAIL ***") << "  ";
    printGrid(got);
    cout << "\n";
}

void runC(const string &name, BoardFiller &f, vector<vector<char>> g, int k, string expectedFlat)
{
    auto got = f.fillLexSmallest(g);
    string flat;
    for (auto &row : got)
        for (char c : row)
            flat += c;
    bool ok = (flat == expectedFlat) && (expectedFlat.empty() || validate(g, got, k));
    cout << name << ": got=\"" << flat << "\" expected=\"" << expectedFlat << "\""
         << (ok ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main()
{
    // ---- Q(a) ----
    {
        BoardFiller f(3);
        runA("a_2x2_empty", f, {{'.', '.'}, {'.', '.'}}, true);
        runA("a_prefilled", f, {{'A', '.'}, {'.', '.'}}, true);
        runA("a_single", f, {{'.'}}, true);
    }
    {
        BoardFiller f(1); // 只有 1 种类别 'A'
        // 2x2 用 1 种类别不可能(相邻必撞), 除非 1x1
        runA("a_k1_2x2", f, {{'.', '.'}, {'.', '.'}}, false);
        runA("a_k1_1x1", f, {{'.'}}, true);
    }
    {
        BoardFiller f(3);
        // 预填本身就冲突: 两个相邻 A -> 无解
        runA("a_conflict", f, {{'A', 'A'}, {'.', '.'}}, false);
    }

    // ---- Q(b) ----
    {
        BoardFiller f(3);
        runB("b_2x2", f, {{'.', '.'}, {'.', '.'}}, 3, true);
        runB("b_prefill", f, {{'A', '.'}, {'.', '.'}}, 3, true);
        runB("b_k1_fail", f, {{'.', '.'}, {'.', '.'}}, /*但 f 的 k=3*/ 3, true);
    }
    {
        BoardFiller f(2);
        runB("b_k2_2x2", f, {{'.', '.'}, {'.', '.'}}, 2, true); // 棋盘 AB/BA
        runB("b_conflict", f, {{'A', 'A'}, {'.', '.'}}, 2, false);
    }

    // ---- Q(c) 字典序最小 ----
    {
        BoardFiller f(3);
        // 2x2 全空, k=3, 字典序最小:
        //   (0,0)填A -> (0,1)不能A,填B -> (1,0)不能A(上),填B -> (1,1)不能B(上)不能B(左),填A
        //   -> "ABBA"
        runC("c_2x2", f, {{'.', '.'}, {'.', '.'}}, 3, "ABBA");
        // 预填 A 在 (0,0):
        //   (0,1)填B, (1,0)填B, (1,1)填A -> "ABBA"
        runC("c_prefill", f, {{'A', '.'}, {'.', '.'}}, 3, "ABBA");
    }
    {
        BoardFiller f(2);
        runC("c_k2", f, {{'.', '.'}, {'.', '.'}}, 2, "ABBA");
    }

    return 0;
}