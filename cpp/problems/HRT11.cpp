#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <queue>
#include <algorithm>
using namespace std;

class Counter {
private:
    unordered_map<string, int> counts_;

public:
    void add(const string& key) {
        counts_[key]++;
    }

    int get(const string& key) const {
        auto it = counts_.find(key);
        return it == counts_.end() ? 0 : it->second;
    }

    // TODO
    /*
规则：

返回出现次数最高的 k 个不同 key
次数高的排前面
如果次数相同，按字符串字典序升序
如果 k <= 0，返回空
如果 k > distinct key 数量，返回所有 key
不允许修改 counts_
*/
/*
state:
  小顶堆： pq<<count, key>>
          计较算法呢， A，B A.count < B.count A 小 A.count ==B.count A.key > B.key, A 小
               小pair    2 def, 2 abc  大pair 
算法：
  对于每一个 key，count
    如果 pq.size() < k push
    否则  如果 count > pq.top().count, pop, push({count, key})
          如果 count == pq.top().count, 并且 key < pq.top().key,pop, push{count, key}
  最后弹出所有堆中所有元素，然后reverse
*/
    vector<string> topK(int k) const {
        if (k<=0) return {};
        auto cmp = [](pair<int, string> &a, pair<int, string> &b) {
            if (a.first == b.first) {
                return a.second < b.second;
            }
            return a.first > b.first;
        };
        priority_queue<pair<int, string>, vector<pair<int, string>>, decltype(cmp)> pq(cmp);
        for (auto & [key, value] : counts_) {
            if (pq.size()<k) {
                pq.push({value, key});
                continue;
            }
            if (value > pq.top().first || (value == pq.top().first && key < pq.top().second)) {
                pq.pop();
                pq.push({value, key});
            }
        }
        vector<string> ret;
        while (!pq.empty()) {
            ret.push_back(pq.top().second);
            pq.pop();
        }
        reverse(ret.begin(), ret.end());
        return ret;
    }
};

int main() {
    {
        Counter c;
        assert(c.topK(3).empty());
    }

    {
        Counter c;
        c.add("a");
        c.add("b");
        c.add("a");
        c.add("c");
        c.add("b");
        c.add("a");

        assert((c.topK(1) == vector<string>{"a"}));
        assert((c.topK(2) == vector<string>{"a", "b"}));
        assert((c.topK(10) == vector<string>{"a", "b", "c"}));
    }

    {
        Counter c;
        c.add("dog");
        c.add("cat");
        c.add("apple");

        // same count -> lexicographical order
        assert((c.topK(3) ==
                vector<string>{"apple", "cat", "dog"}));
    }

    {
        Counter c;
        c.add("x");
        c.add("y");
        c.add("x");
        c.add("y");
        c.add("z");

        assert((c.topK(3) ==
                vector<string>{"x", "y", "z"}));
    }

    {
        Counter c;
        c.add("a");

        assert(c.topK(0).empty());
        assert(c.topK(-5).empty());
    }

    cout << "All tests passed!\n";
}