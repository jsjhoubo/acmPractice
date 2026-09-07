#include <bits/stdc++.h>
#include <unordered_map>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS.
//  Return closed interval [i, j] covering every query token.
//  No valid window  -> return {-1, -1}.
// ============================================================
pair<int,int> best_snippet(const vector<string>& doc,
                           const vector<string>& query) {
    // TODO: your code here
    size_t i=0;
    size_t j=0;
    size_t min_len =INT_MAX;
    pair<int, int> ret ={-1,-1};
    unordered_map<string, size_t> fre;
    unordered_map<string, size_t> vocab;
    for (auto & str: query) {
        vocab[str]=1;
    }
    size_t total =0;
    while (j< doc.size()) {
        while (j<doc.size() ) {
            if (total ==vocab.size()) {
                break;
            }
            if (vocab.find(doc[j])!=vocab.end()) {
                fre[doc[j]]++;
                if (fre[doc[j]] <= vocab[doc[j]]) {
                    ++total;
                }
            }
            j++;
        }
        while (i<j && total== vocab.size()) {
            if (j-i<min_len) {
                min_len =j-i;
                ret ={(int)i, (int)(j-1)};
            }
            if (vocab.find(doc[i])!=vocab.end()) {
                --fre[doc[i]];
                if(fre[doc[i]] < vocab[doc[i]]) {
                    --total;
                }
            }
            ++i;
        }
    }
    return ret;
}

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed = 0, failed = 0;

void check(const string& name,
           const vector<string>& doc,
           const vector<string>& query,
           pair<int,int> expectLen /* {-1,-1} means no-window; else {expI,expJ} OR len-check */,
           bool lenOnly = false,
           int expLen = -1) {
    auto got = best_snippet(doc, query);
    bool ok;
    if (lenOnly) {
        // accept any valid shortest window of the right length
        int gl = (got.first < 0) ? -1 : got.second - got.first + 1;
        ok = (gl == expLen);
    } else {
        ok = (got == expectLen);
    }
    if (ok) { passed++; printf("  [PASS] %s  -> (%d,%d)\n", name.c_str(), got.first, got.second); }
    else    { failed++; printf("  [FAIL] %s  -> got (%d,%d)\n", name.c_str(), got.first, got.second); }
}

int main() {
    // 1) spec example: answer window length 4 -> [4,7]
    check("spec_example",
          {"the","quick","brown","fox","quick","lazy","dog","brown"},
          {"quick","brown","dog"},
          {-1,-1}, /*lenOnly*/true, /*expLen*/4);

    // 2) single token query
    check("single_token",
          {"a","b","c","b","d"}, {"b"},
          {-1,-1}, true, 1);

    // 3) whole doc needed
    check("need_whole_doc",
          {"a","b","c"}, {"a","b","c"},
          {-1,-1}, true, 3);

    // 4) no valid window (query token absent)
    check("absent_token",
          {"a","b","c"}, {"a","z"},
          {-1,-1});

    // 5) duplicates galore, tight window late
    check("tight_window_late",
          {"x","a","b","x","x","a","b"}, {"a","b"},
          {-1,-1}, true, 2);

    // 6) answer at very start
    check("answer_at_start",
          {"a","b","junk","junk"}, {"a","b"},
          {-1,-1}, true, 2);

    // 7) empty doc
    check("empty_doc",
          {}, {"a"},
          {-1,-1});

    // 8) repeated query token shrinks need to 1 distinct
    //    (query has dup -> distinct set {a}); window len 1
    check("query_has_dup",
          {"z","a","z"}, {"a","a"},
          {-1,-1}, true, 1);

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed == 0 ? 0 : 1;
}