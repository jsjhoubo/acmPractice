// Coupang Staff Round 1 - string count structure, avg O(1) all ops.
// Fill in the private members and the four methods. Then: g++ -std=c++17 -O0 -g allone.cpp && ./a.out

#include <cassert>
#include <iostream>
#include <list>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
using namespace std;
class AllOne {
public:
    AllOne() {
        // TODO
    }
    list<pair<int, unordered_set<string>>> buf;
   
    unordered_map<string, list<pair<int, unordered_set<string>>>::iterator> m1;
    // Increments the count of key by 1. If key is absent, its count becomes 1.
    void inc(const std::string& key) {
        // TODO
        // 如果m1不包含 key，
        //   看下buf 头是不是1，是1插入， 不是则build 一个头 {1, {string}} m1 = buf.begin()
        // 否则
        //  auto iter =m1[key]
        //   m1[key].second.erase(key);
        //   如果m1[key]下一个是空，或者下一个的数字不等于当下数字+1
        //        对应iter 后加入 {数字+1， {key}}
        //   否则
        //       m1[key]下一个的set 插入key
        //   m1[key] = iter的下一个
        //   如果iter 的set 为空 删除这个iter
        if (m1.count(key) ==0) {
            if (buf.size()>0 && buf.front().first ==1) {
                buf.front().second.insert(key);
            }
            else {
                buf.push_front({1, {key}});
            }
            m1[key]=buf.begin();
        }
        else {
            auto iter =m1[key];
            iter->second.erase(key);
            auto next_iter =next(iter);
            if (next_iter == buf.end() || next_iter->first != iter->first +1) {
                buf.insert(next_iter, {iter->first+1, {key}});
            }
            else {
                next_iter->second.insert(key);
            }
            m1[key] =next(iter);
            if (iter->second.size()==0) {
                buf.erase(iter);
            }
        }
    }

    // Decrements the count of key by 1.
    void dec(const std::string& key) {
        // TODO
        // 如果m1不包含 key，
        //   错误不应该，只要插入就应该有
        // 否则
        //  auto iter =m1[key]
        //   m1[key].second.erase(key);
        //   如果是0 要注意下
        //   如果m1[key]前一个是空，或者前一个的数字不等于当下数字-1 并且当下数字减一不等于0 
        //        对应iter 前插入 {数字-1， {key}}
        //   否则
        //       m1[key]前一个的set 插入key
        //   m1[key] = iter的前一个
        //   如果iter 的set 为空 删除这个iter
        auto iter =m1[key];
        iter->second.erase(key);
        if (iter->first -1 !=0) {
            if (iter== buf.begin()) {
                buf.push_front({iter->first-1, {key}});
                m1[key] =buf.begin();
            }
            else {
                auto prev_iter =prev(iter);
                if (prev_iter->first != iter->first-1) {
                    buf.insert(iter, {iter->first-1, {key}});
                }
                else {
                    prev_iter->second.insert(key);
                }
                m1[key] =prev(iter);
            }
        }
        else {
            m1.erase(key);
        }
        if (iter->second.size() ==0) {
            buf.erase(iter);
        }
    }

    // Returns one of the keys with the maximal count.
    std::string getMaxKey() {
        // TODO
        // buf size kong 返回空
        // 返回buf 最后一个指针对应的某个string
        if (buf.size() !=0) {
            return *buf.back().second.begin();
        }
        return "";

    }

    // Returns one of the keys with the minimal count.
    std::string getMinKey() {
        // TODO
        // buf size kong 返回空
        // 返回buf 第一个指针对应的某个string
        if (buf.size() !=0) {
            return *buf.front().second.begin();
        }
        return "";
    }

private:
    // TODO
};

// ---------------------------------------------------------------- test harness

static int failures = 0;

// Ties are legal: assert the answer is *one of* the acceptable keys.
static void expectMax(AllOne& a, const std::set<std::string>& ok, const char* tag) {
    std::string got = a.getMaxKey();
    if (!ok.count(got)) {
        std::cout << "FAIL [" << tag << "] getMaxKey() = \"" << got << "\", expected one of {";
        for (const auto& s : ok) std::cout << "\"" << s << "\" ";
        std::cout << "}\n";
        ++failures;
    }
}

static void expectMin(AllOne& a, const std::set<std::string>& ok, const char* tag) {
    std::string got = a.getMinKey();
    if (!ok.count(got)) {
        std::cout << "FAIL [" << tag << "] getMinKey() = \"" << got << "\", expected one of {";
        for (const auto& s : ok) std::cout << "\"" << s << "\" ";
        std::cout << "}\n";
        ++failures;
    }
}

int main() {
    {   // single key
        AllOne a;
        a.inc("a");
        expectMax(a, {"a"}, "single/max");
        expectMin(a, {"a"}, "single/min");
    }

    {   // two keys, distinct counts
        AllOne a;
        a.inc("a"); a.inc("a"); a.inc("a");   // a=3
        a.inc("b");                            // b=1
        expectMax(a, {"a"}, "distinct/max");
        expectMin(a, {"b"}, "distinct/min");
    }

    {   // tie: both answers legal
        AllOne a;
        a.inc("a"); a.inc("b");
        expectMax(a, {"a", "b"}, "tie/max");
        expectMin(a, {"a", "b"}, "tie/min");
    }

    {   // dec to zero removes the key entirely
        AllOne a;
        a.inc("a"); a.inc("b"); a.inc("b");    // a=1, b=2
        a.dec("a");                             // a gone
        expectMax(a, {"b"}, "removal/max");
        expectMin(a, {"b"}, "removal/min");
    }

    {   // node with a gap: counts 1 and 5, nothing between
        AllOne a;
        for (int i = 0; i < 5; ++i) a.inc("hi");
        a.inc("lo");
        expectMax(a, {"hi"}, "gap/max");
        expectMin(a, {"lo"}, "gap/min");
        a.inc("lo"); a.inc("lo");              // lo=3, still a gap on both sides
        expectMax(a, {"hi"}, "gap/max2");
        expectMin(a, {"lo"}, "gap/min2");
    }

    {   // walk a key all the way up and back down through shared nodes
        AllOne a;
        a.inc("x"); a.inc("y");                // both 1
        for (int i = 0; i < 4; ++i) a.inc("x"); // x=5, y=1
        expectMax(a, {"x"}, "walk/max");
        expectMin(a, {"y"}, "walk/min");
        for (int i = 0; i < 4; ++i) a.dec("x"); // x=1, y=1
        expectMax(a, {"x", "y"}, "walk/back-max");
        expectMin(a, {"x", "y"}, "walk/back-min");
        a.dec("x");                             // x gone
        expectMax(a, {"y"}, "walk/gone-max");
        expectMin(a, {"y"}, "walk/gone-min");
    }

    {   // re-add after full removal
        AllOne a;
        a.inc("z"); a.dec("z");                 // empty
        a.inc("z");
        expectMax(a, {"z"}, "readd/max");
        expectMin(a, {"z"}, "readd/min");
    }

    {   // many keys sharing one count, then one breaks away
        AllOne a;
        for (const char* k : {"p", "q", "r", "s"}) a.inc(k);
        expectMax(a, {"p", "q", "r", "s"}, "shared/max");
        a.inc("q");
        expectMax(a, {"q"}, "shared/broke-away");
        expectMin(a, {"p", "r", "s"}, "shared/min");
    }

    // ---- behaviour below depends on clarifications you should ASK the interviewer.
    // Uncomment once you've decided the contract.

    // {   // empty structure
    //     AllOne a;
    //     expectMax(a, {""}, "empty/max");
    //     expectMin(a, {""}, "empty/min");
    // }

    // {   // dec on a key that isn't present
    //     AllOne a;
    //     a.inc("a");
    //     a.dec("nope");        // no-op? or UB by contract?
    //     expectMax(a, {"a"}, "dec-absent/max");
    //     expectMin(a, {"a"}, "dec-absent/min");
    // }

    std::cout << (failures ? "FAILURES: " : "all passed, failures: ") << failures << "\n";
    return failures != 0;
}