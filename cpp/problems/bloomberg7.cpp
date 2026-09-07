#include <bits/stdc++.h>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS.
//  Wildcard match over the WHOLE title.
//    '?' matches any single char
//    '*' matches any sequence (including empty)
//  dp[i][j] = title[:i] matches pattern[:j]   (i,j are LENGTHS)
//    base: dp[0][0] = true
//    '*' :  dp[i][j] = dp[i-1][j] || dp[i][j-1]
//    else:  dp[i][j] = dp[i-1][j-1] && (pat==title || pat=='?')
//  Return bool.
// ============================================================
bool wildcard_match(const string& title, const string& pattern) {

    // TODO: your code here
    size_t n =title.size();
    size_t m =pattern.size();
    
    vector<vector<bool>> dp(n+1, vector<bool>(m+1, false));
    dp[0][0] =true;
    for (int j=1;j<=m;j++) {
        char ch =pattern[j-1];
        dp[0][j] =dp[0][j-1] && ch =='*'; 
    }
    for (size_t i=1;i<=n;i++) {
        for (size_t j=1;j<=m;j++) {
            char ch1 = title[i-1];
            char ch2 = pattern[j-1];
            if (ch2 =='*') {
                dp[i][j] =dp[i-1][j] || dp[i][j-1];
            }
            else if (ch2 =='?') {
                dp[i][j] =dp[i-1][j-1];
            }
            else if(ch1 ==ch2) {
                    dp[i][j] =dp[i-1][j-1];
            }
        }
    }
    return dp[n][m];
}

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed = 0, failed = 0;
static void check(const string& name, const string& title,
                  const string& pattern, bool expected) {
    bool got = wildcard_match(title, pattern);
    if (got == expected) {
        passed++;
        printf("  [PASS] %s: match(\"%s\",\"%s\") = %s\n",
               name.c_str(), title.c_str(), pattern.c_str(), got?"true":"false");
    } else {
        failed++;
        printf("  [FAIL] %s: match(\"%s\",\"%s\") = %s, expected %s\n",
               name.c_str(), title.c_str(), pattern.c_str(),
               got?"true":"false", expected?"true":"false");
    }
}

int main() {
    // spec examples
    check("star_mid",     "apple", "a*e",   true);
    check("qmark",        "apple", "a?ple", true);
    check("star_nomatch", "apple", "a*f",   false);
    check("empty_star",   "",      "*",     true);
    check("star_suffix",  "abc",   "*c",    true);
    check("qmark_star",   "abc",   "?*",    true);

    // boundaries
    check("empty_empty",    "",      "",        true);
    check("empty_pat",      "abc",   "",        false);
    check("title_empty_q",  "",      "?",       false);
    check("all_star",       "abc",   "***",     true);
    check("star_collapse",  "abc",   "*a*b*c*", true);
    check("exact",          "abc",   "abc",     true);
    check("exact_no",       "abc",   "abd",     false);
    check("q_full",         "abc",   "???",     true);
    check("q_toomany",      "abc",   "????",    false);
    check("star_then_q",    "abcd",  "a*?d",    true);
    check("lead_star",      "xabc",  "*abc",    true);
    check("only_star_long", "abcdef","*",       true);
    check("trailing",       "abc",   "abc*",    true);
    check("double_star_mid","abcde", "a**de",   true);

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed == 0 ? 0 : 1;
}