#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <functional>

using namespace std;


// ====== YOUR CLASS HERE ======
class BannedWordFilter
{
public:
    BannedWordFilter(vector<string> &spams)
    {
        _root = new Trie();
        for (auto word : spams)
        {
            addWord(word);
        }
    }

    ~BannedWordFilter()
    {
        deleteTrie();
    }

    bool containSpam(string sentence, bool partial)
    {
        int n = sentence.size();
        int i = 0;
        int j = 0;
        auto valid = [&](int x)
        {
            return (sentence[x] >= 'a' && sentence[x] <= 'z') || (sentence[x] >= 'A' && sentence[x] <= 'Z');
        };
        while (i < n && valid(i) == false)
        {
            i++;
        }
        if (i == n)
        {
            return false;
        }
        while (i < n)
        {
            while (j < n && valid(j))
            {
                j++;
            }
            if (j == i)
            {
                return false;
            }
            bool match = find(sentence.substr(i, j - i), partial);
            if (match)
            {
                return match;
            }
            i = j;
            while (i < n && valid(i) == false)
            {
                i++;
            }
            if (i == n)
            {
                return false;
            }
            j = i;
        }
        return false;
    }

private:
    struct Trie
    {
        unordered_map<char, Trie *> _children;
        bool _end;
        Trie()
        {
            _end = false;
        }
    };
    Trie *_root;
    void addWord(string word)
    {
        std::function<void(int, Trie *)> dfs = [&](int d, Trie *r)
        {
            if (d == word.size())
            {
                r->_end = true;
                return;
            }
            if (r->_children.find(word[d]) == r->_children.end())
            {
                r->_children[word[d]] = new Trie();
            }
            dfs(d + 1, r->_children[word[d]]);
        };
        dfs(0, _root);
    }

    void deleteTrie()
    {
        std::function<void(Trie *)> helper = [&](Trie *r)
        {
            for (auto &[ch, node] : r->_children)
            {
                helper(node);
            }
            delete r;
        };
        helper(_root);
    }

    bool find(string word, bool partial)
    {
        std::function<bool(int, Trie *)> helper = [&](int d, Trie *r) -> bool
        {
            if (d == word.size())
            {
                if (partial)
                {
                    return true;
                }
                return r->_end;
            }
            if (r->_children.find(word[d]) == r->_children.end())
            {
                return false;
            }
            return helper(d + 1, r->_children[word[d]]);
        };
        return helper(0, _root);
    }
};

// ============ TEST HARNESS ============
// 你定好 API 后,把下面的调用改成你的接口,跑测试验证。
void run(const string& name, BannedWordFilter& f, string sentence, bool partial, bool expected) {
    bool got = f.containSpam(sentence, partial);
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main() {
    vector<string> banned = {"spam", "scam", "fake"};
    BannedWordFilter f(banned);

    // ---- 整词匹配 (partial = false) ----
    run("exact_hit",        f, "this is spam",        false, true);   // 整词 spam
    run("exact_none",       f, "buy now great deal",  false, false);
    run("exact_fake",       f, "fake news today",     false, true);
    run("exact_substr_no",  f, "spammer is here",     false, false);  // spammer != spam (整词)
    run("exact_multi",      f, "spam spam spam",      false, true);
    run("exact_only_word",  f, "scam",                false, true);
    run("exact_empty",      f, "",                    false, false);
    run("exact_punct",      f, "hello, spam!",        false, true);   // 标点分词后剩 spam
    run("exact_no_letters", f, "123 456 !!!",         false, false);

    // ---- 子串/前缀匹配 (partial = true) ----
    // 注意:你的 partial 逻辑是 "word 沿 trie 走到 word 末尾就 true"
    //       => 实际是 "word 是某敏感词的前缀" 才 true (因为是拿 word 去走 trie)
    run("partial_prefix",   f, "spa is short",        true,  true);   // "spa" 是 spam 前缀 -> true
    run("partial_full",     f, "this is spam",        true,  true);   // spam 走通 -> true
    run("partial_longer",   f, "spammer here",        true,  false);  // "spammer" 走到 spam 后 'm'..'e' 无路 -> false
    run("partial_none",     f, "buy now",             true,  false);

    return 0;
}