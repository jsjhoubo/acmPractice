#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <queue>
#include <algorithm>
using namespace std;

class TrendingPins
{
public:
    static const int WINDOW = 3600;

    map<int, map<string, int>> stat;

    void recordSave(const string &pin_id, int ts)
    {
        stat[ts][pin_id]++;
    }

    vector<string> getTopK(int ts, int K)
    {
        if (K <= 0) {                          // [改] K==0 -> K<=0,顺便挡负数
            return {};
        }
        map<string, int> fre;
        for (auto it = stat.lower_bound(ts - WINDOW); it != stat.end(); ++it)
        {
            if (it->first > ts)
            {
                break;
            }
            for (auto &[pin, cnt] : it->second)
            {
                fre[pin] += cnt;
            }
        }

        // [改] 比较器从 >= 改成严格 > (strict weak ordering,>= 是 UB)
        // 最小堆:堆顶=最该淘汰的。count 小的淘汰;count 相同时字典序大的淘汰
        auto cmp = [](const pair<int, string> &left, const pair<int, string> &right)
        {
            if (left.first != right.first)
                return left.first > right.first; 
            return left.second < right.second;   // [改] >= -> >
                    // [改] >= -> >
        };
        priority_queue<pair<int, string>, vector<pair<int, string>>, decltype(cmp)> pq(cmp);
        for (auto &[pin, cnt] : fre)
        {
            if ((int)pq.size() < K)                  // [改] 加 (int) 强转,避免 size_t/int 比较警告
            {
                pq.push({cnt, pin});
            }
            else
            {
                if (cnt > pq.top().first)
                {
                    pq.pop();
                    pq.push({cnt, pin});
                }
            }
        }
        vector<string> ret;
        while (!pq.empty())
        {
            ret.push_back(pq.top().second);
            pq.pop();
        }
        reverse(ret.begin(), ret.end());
        return ret;
    }
};

// ============ TEST HARNESS ============
void run(const string &name, vector<string> got, vector<string> expected)
{
    bool pass = (got == expected);
    cout << name << ": got=[";
    for (auto &s : got) cout << s << " ";
    cout << "] expected=[";
    for (auto &s : expected) cout << s << " ";
    cout << "]" << (pass ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main()
{
    { TrendingPins t; t.recordSave("A",100); t.recordSave("B",200); t.recordSave("A",300);
      run("basic_top2", t.getTopK(300,2), {"A","B"}); }

    { TrendingPins t; t.recordSave("A",100); t.recordSave("B",1000); t.recordSave("B",2000);
      run("expire_A", t.getTopK(4000,2), {"B"}); }

    { TrendingPins t; t.recordSave("C",100); t.recordSave("A",100); t.recordSave("B",100);
      run("tie_break_allfit", t.getTopK(100,3), {"A","B","C"}); }

    { TrendingPins t; t.recordSave("A",50);
      run("k_too_big", t.getTopK(50,5), {"A"}); }

    { TrendingPins t; t.recordSave("A",100);
      run("all_expired", t.getTopK(5000,3), {}); }

    { TrendingPins t; t.recordSave("A",400); t.recordSave("B",500);
      run("boundary", t.getTopK(4000,2), {"A","B"}); }

    // 狠 case
    { TrendingPins t; t.recordSave("D",100); t.recordSave("C",100);
      t.recordSave("B",100); t.recordSave("A",100);
      run("tie_eliminate", t.getTopK(100,2), {"A","B"}); }

    { TrendingPins t;
      t.recordSave("A",1); t.recordSave("A",2); t.recordSave("A",3);
      t.recordSave("C",1); t.recordSave("C",2);
      t.recordSave("B",1); t.recordSave("B",2);
      t.recordSave("D",1);
      run("count_plus_tie", t.getTopK(3600,2), {"A","B"}); }

    { TrendingPins t; t.recordSave("A",100); t.recordSave("A",4000); t.recordSave("B",4000);
      run("partial_expire_samepin", t.getTopK(4000,2), {"A","B"}); }

    { TrendingPins t; t.recordSave("A",100);
      run("k_zero", t.getTopK(100,0), {}); }

    { TrendingPins t; t.recordSave("A",400);
      run("boundary_exact", t.getTopK(4000,1), {"A"}); }

    { TrendingPins t;
      for (int i=0;i<5;i++) t.recordSave("A",100);
      for (int i=0;i<3;i++) t.recordSave("B",100);
      run("same_ts_counts", t.getTopK(100,2), {"A","B"}); }

    return 0;
}