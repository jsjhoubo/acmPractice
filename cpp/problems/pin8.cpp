#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <algorithm>
using namespace std;

class QuerySegmenter {
private:
    struct Trie {
        unordered_map<char , Trie*> children;
        bool end;
        Trie () {
            end =false;
        }
    };
    int _maxLen;
    Trie* _root;
    vector<vector<int>> _prev;
public:
    

    QuerySegmenter(vector<string>& dict) {
        _maxLen=0;
        _root =new Trie();
        // 建 trie + 记录 maxLen
        for (auto word : dict) {
            _maxLen =max((int)word.size(), _maxLen);
            Trie* t =_root;
            for (auto ch: word) {
                if (t->children.find(ch) == t->children.end()) {
                    t->children[ch] =new Trie();
                }
                t = t->children[ch];
            }
            t->end =true;
        }
    }

    // Q(a): 能否切分
    bool canSegment(const string& s) {
        // 你写: DP, j 只到 maxLen
        _prev.clear(); 
        vector<bool> dp(s.size()+1, false);
        dp[0] =true;
        std::function<bool(int,Trie*, string)> find =[&](int d, Trie* r, const string & word) ->bool {
            if (r ==nullptr) {
                return false;
            }
            if (d ==word.size()) {
                return r->end;
            }
            if (r->children.find(word[d])==r->children.end()) {
                return false;
            }
            return find(d+1, r->children[word[d]], word);
        };
        _prev.assign(s.size() + 1, {});
        for (int i=0;i<s.size();i++) {
            int j =i+1;
            for (int k=1;k<=_maxLen && k<=j;k++) {
                if (dp[j-k] && find(0, _root, s.substr(j-k, k))) {
                    dp[j] =true;
                    _prev[j-1].push_back(j-k);
                }
            }
        }
        return dp[s.size()];
    }

    // Q(b): 所有切分方式
    vector<vector<string>> allSegments(const string& s) {
        // 你写: DFS + 记忆化
        bool can = canSegment(s);
        vector<vector<string>> ret;
        if (!can) {
            return ret;
        }
        std::function<void(int,vector<string> &)> helper =[&](int d, vector<string> &path) {
            if (d <0) {
                ret.push_back(path);
                reverse(ret.back().begin(), ret.back().end());
                return;
            }
            if (_prev[d].size()>0) {
                for (auto i: _prev[d]) {
                    path.push_back(s.substr(i, d -i+1));
                    helper(i-1, path);
                    path.pop_back();
                }
            }
        };
        vector<string> path;
        helper(s.size()-1, path);
        return ret;
    }


};


// ============ TEST HARNESS ============
void runA(const string& name, QuerySegmenter& seg, string s, bool expected) {
    bool got = seg.canSegment(s);
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

void printSegs(const vector<vector<string>>& v) {
    cout << "{";
    for (auto& seg : v) {
        cout << "[";
        for (auto& w : seg) cout << w << " ";
        cout << "]";
    }
    cout << "}";
}

void runB(const string& name, QuerySegmenter& seg, string s, int expectedCount) {
    auto got = seg.allSegments(s);
    cout << name << ": count=" << got.size() << " expected=" << expectedCount
         << (got.size() == expectedCount ? "  PASS" : "  *** FAIL ***") << "  ";
    printSegs(got);
    cout << "\n";
}

int main() {
    // ===== Q(a) canSegment =====
    {
        vector<string> dict = {"pin","interest","pinterest","board","art"};
        QuerySegmenter seg(dict);
        runA("a_basic",      seg, "pinterestboard", true);   // pinterest|board
        runA("a_two_ways",   seg, "pininterest",    true);   // pin|interest
        runA("a_single",     seg, "art",            true);
        runA("a_fail",       seg, "pinterestx",     false);  // 末尾 x 无法切
        runA("a_empty",      seg, "",               true);   // 空串 -> dp[0]=true (确认你想要的语义)
        runA("a_partial",    seg, "pinboard",       true);   // pin|board
        runA("a_nomatch",    seg, "xyz",            false);
    }

    // ===== Q(b) allSegments =====
    {
        vector<string> dict = {"pin","interest","pinterest"};
        QuerySegmenter seg(dict);
        // "pinterest" -> [pin,interest] 和 [pinterest] = 2 种
        runB("b_two_ways",   seg, "pinterest",      2);
    }
    {
        vector<string> dict = {"cat","cats","and","sand","dog"};
        QuerySegmenter seg(dict);
        // LC140 经典: "catsanddog" -> [cats,and,dog] [cat,sand,dog] = 2 种
        runB("b_catsanddog", seg, "catsanddog",     2);
    }
    {
        vector<string> dict = {"a","aa","aaa"};
        QuerySegmenter seg(dict);
        // "aaa" -> [a,a,a][a,aa][aa,a][aaa] = 4 种 (指数爆炸压力测试)
        runB("b_exponential",seg, "aaa",            4);
    }
    {
        vector<string> dict = {"pin","interest","pinterest","board"};
        QuerySegmenter seg(dict);
        // "pinterestboard" -> [pinterest,board] 和 [pin,interest,board] = 2 种
        runB("b_with_board", seg, "pinterestboard", 2);
    }
    {
        vector<string> dict = {"pin","board"};
        QuerySegmenter seg(dict);
        // 无法切分 -> 0 种
        runB("b_nomatch",    seg, "pinx",           0);
    }

    return 0;
}