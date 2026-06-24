#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <algorithm>
using namespace std;

// ============================================================
// Hard Q1: 找出 comment 里所有敏感词出现的位置(子串,任意位置),
//          把重叠/相邻的命中区间合并,返回需打码的 [start, end) 列表。
//
//   banned = ["spam","scam","ham"]
//   comment = "freehamspamx"   (h在4, ham@[4,7), spam@[7,11), 相邻->合并)
//   -> [[4,11)]
// ============================================================

class Censor {
public:
    Censor(vector<string>& banned) {
        _root = new Trie();
        for (auto& w : banned) addWord(w);
    }
    ~Censor() { /* 省略 delete,面试可口头说 */ }

    // 返回合并后的需打码区间 [start, end)
    vector<pair<int,int>> maskIntervals(const string& comment) {
        vector<pair<int,int>> merged;
        auto initial = findAllMatches(comment);
        if (initial.size() ==0) {
            return merged;
        }
        merged.push_back(initial[0]);
        for (int i=1;i<initial.size();i++) {
            if (initial[i].first <= merged.back().second) {
                merged.back().second =max(initial[i].second, merged.back().second);
            }
            else {
                merged.push_back(initial[i]);
            }
        }
        return merged;
    }

private:
    struct Trie {
        unordered_map<char, Trie*> _children;
        bool _end = false;
    };
    Trie* _root;

    void addWord(const string& word) {
        Trie* r = _root;
        for (char c : word) {
            if (!r->_children.count(c)) r->_children[c] = new Trie();
            r = r->_children[c];
        }
        r->_end = true;
    }
    void find(string word, vector<int> &lens)
    {
        std::function<void(int, Trie *)> helper = [&](int d, Trie *r)
        {
            if (r->_end) {
                lens.push_back(d);
            }
            if (d == word.size())
            {
                return;
            }
            if (r->_children.find(word[d]) == r->_children.end())
            {
                return;
            }
            helper(d + 1, r->_children[word[d]]);
        };
        helper(0, _root);
    }
    // 你可能需要的 helper: 从 comment[start] 起,收集所有命中的结束位置
    // (留给你设计)
    vector<pair<int, int>> findAllMatches(const string & comment) {
        vector<pair<int, int>> ret;
        int n = comment.size();
        int i = 0;
        int j = 0;
        auto valid = [&](int x)
        {
            return (comment[x] >= 'a' && comment[x] <= 'z') || (comment[x] >= 'A' && comment[x] <= 'Z');
        };
        while (i < n && valid(i) == false)
        {
            i++;
        }
        while (i < n)
        {
            while (j < n && valid(j))
            {
                j++;
            }
            for (int k=i;k<j;k++) {
                vector<int> lens;
                find(comment.substr(k, j - k), lens);
                if (lens.size() >0) {
                    ret.push_back({k, lens.back() + k});
                }
            }
            i = j;
            while (i < n && valid(i) == false)
            {
                i++;
            }
            j = i;
        }
        return ret;
    }
};

// ============ TEST HARNESS ============
void printIv(const vector<pair<int,int>>& v) {
    cout << "[";
    for (auto& [a,b] : v) cout << "[" << a << "," << b << ")";
    cout << "]";
}

void run(const string& name, vector<string> banned, string comment,
         vector<pair<int,int>> expected) {
    Censor c(banned);
    auto got = c.maskIntervals(comment);
    sort(got.begin(), got.end());
    bool pass = (got == expected);
    cout << name << ": got="; printIv(got);
    cout << " expected="; printIv(expected);
    cout << (pass ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main() {
    // 1) 相邻合并: ham@[4,7) + spam@[7,11) -> [4,11)
    run("adjacent_merge", {"spam","scam","ham"}, "freehamspamx", {{4,11}});

    // 2) 重叠合并: "spamscam" spam@[0,4) scam@[4,8) 相邻 -> [0,8)
    run("touching", {"spam","scam"}, "spamscam", {{0,8}});

    // 3) 真重叠: banned 含 "abc","bcd" -> "abcd" abc@[0,3) bcd@[1,4) 重叠 -> [0,4)
    run("overlap", {"abc","bcd"}, "abcd", {{0,4}});

    // 4) 分离的两段: "spamXXXham" -> [0,4) 和 [7,10)
    run("separate", {"spam","ham"}, "spamxxxham", {{0,4},{7,10}});

    // 5) 无命中
    run("none", {"spam"}, "hello world", {});

    // 6) 整条就是敏感词
    run("whole", {"spam"}, "spam", {{0,4}});

    // 7) 嵌入中间: "xxspamxx" -> [2,6)
    run("embedded", {"spam"}, "xxspamxx", {{2,6}});

    // 8) 空评论
    run("empty", {"spam"}, "", {});

    // 9) 同起点多词(短词被长词覆盖): banned "ham","hamster" -> "hamster"
    //    ham@[0,3) hamster@[0,7) -> 合并 [0,7)
    run("nested", {"ham","hamster"}, "hamster", {{0,7}});

    return 0;
}