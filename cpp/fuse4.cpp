#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <stdexcept>
#include <algorithm>
#include <cassert>
using namespace std;

struct Cell
{
    int val = 0;
    bool isLiteral = false;
    vector<string> deps; // 原始 refs:"B2" 或 "A1:B2"(压缩,不展开)
}; // deps 本身就是图的出边,没有独立 _graph

class SpreadSheet
{
    unordered_map<string, Cell> _cells;
    unordered_set<string> _formulas; // 公式格名单:判环只走它们(literal 无出边)

    static pair<int, int> Coord(const string &s)
    { // "AA10" -> {10,27}
        int r = 0, c = 0;
        for (char ch : s)
        {
            if ('A' <= ch && ch <= 'Z')
                c = c * 26 + (ch - 'A' + 1);
            else if ('0' <= ch && ch <= '9')
                r = r * 10 + (ch - '0');
        }
        return {r, c};
    }
    static string Name(int r, int c)
    { // {10,27} -> "AA10"
        string col;
        while (c > 0)
        {
            int m = c % 26;
            if (m == 0)
            {
                col += 'Z';
                c = c / 26 - 1;
            } // Z 吃借位
            else
            {
                col += char('A' + m - 1);
                c /= 26;
            }
        }
        reverse(col.begin(), col.end());
        return col + to_string(r);
    }
    static bool InRect(const string &cell, const string &ref)
    { // ref = "A1:B2"
        size_t p = ref.find(':');
        auto [r, c] = Coord(cell);
        auto [r1, c1] = Coord(ref.substr(0, p));
        auto [r2, c2] = Coord(ref.substr(p + 1));
        return r1 <= r && r <= r2 && c1 <= c && c <= c2;
    }

    // 判环:从引用 d 出发,沿【现存公式】能否走到 target?(range 永不展开)
    bool Reaches(const string &d, const string &target, unordered_set<string> &vis)
    {
        if (d.find(':') == string::npos)
        { // 单格引用
            if (d == target)
                return true;
            if (_formulas.count(d) == 0 || vis.count(d) > 0)
                return false;
            vis.insert(d);
            for (auto &nd : _cells[d].deps)
                if (Reaches(nd, target, vis))
                    return true;
            return false;
        }
        if (InRect(target, d))
            return true; // range:target 在矩形内 = 够到
        for (auto &f : _formulas)
        { // 只有矩形内的公式格能继续走
            if (!InRect(f, d) || vis.count(f) > 0)
                continue;
            vis.insert(f);
            for (auto &nd : _cells[f].deps)
                if (Reaches(nd, target, vis))
                    return true;
        }
        return false;
    }

    // 求值:纯函数,只返回不写。注意与判环相反:不去重(重复引用按次数累加)、
    // 且要走所有存在格(literal 是求和主体),不只公式格。
    int Eval(const string &cell)
    {
        auto it = _cells.find(cell);
        if (it == _cells.end())
            return 0;
        if (it->second.isLiteral)
            return it->second.val;
        int total = 0;
        for (auto &d : it->second.deps)
        {
            size_t p = d.find(':');
            if (p == string::npos)
            {
                total += Eval(d);
                continue;
            }
            for (auto &[name, cc] : _cells) // (Eval 只读,遍历中递归安全)
                if (InRect(name, d))
                    total += Eval(name);
        }
        return total;
    }

public:
    void Set(const string &cell, int v)
    {
        _cells[cell] = Cell{v, true, {}};
        _formulas.erase(cell); // 身份切换:公式被覆盖成 literal,名单同步
    }
    int Get(const string &cell) { return Eval(cell); }

    void Sum(const string &target, const vector<string> &refs)
    {
        unordered_set<string> vis; // ★ 先检后写:检环时什么都没动,
        for (auto &d : refs)       //   失败 throw 即事务,零回滚
            if (Reaches(d, target, vis))
                throw runtime_error("cycle detected");
        _cells[target] = Cell{0, false, refs};
        _formulas.insert(target);
    }
};

int main()
{
    { // 1. 官方样例(顺带覆盖了大矩形分支:area=4 > 现存格数 3,走扫描路径)
        SpreadSheet s;
        s.Set("A1", 1);
        s.Set("B2", 2);
        s.Sum("D1", {"A1:B2", "B2"});
        assert(s.Get("D1") == 5);
        s.Set("A1", 2);
        assert(s.Get("D1") == 6);
    }
    { // 2. 自引用抛错,表不变
        SpreadSheet s;
        bool thrown = false;
        try
        {
            s.Sum("A1", {"A1"});
        }
        catch (...)
        {
            thrown = true;
        }
        assert(thrown && s.Get("A1") == 0);
    }
    { // 3. 两步环:第二次抛错,事后 Get 一切正常
        SpreadSheet s;
        s.Set("B1", 7);
        s.Sum("A1", {"B1"});
        bool thrown = false;
        try
        {
            s.Sum("B1", {"A1"});
        }
        catch (...)
        {
            thrown = true;
        }
        assert(thrown && s.Get("A1") == 7 && s.Get("B1") == 7);
    }
    { // 4. 覆盖语义双向
        SpreadSheet s;
        s.Set("A1", 3);
        s.Set("D1", 10);
        s.Sum("D1", {"A1"});
        assert(s.Get("D1") == 3); // Sum 覆盖 literal
        s.Set("D1", 9);
        assert(s.Get("D1") == 9); // Set 覆盖公式
    }
    return 0;
}