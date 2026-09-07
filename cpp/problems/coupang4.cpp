// Coupang Sr Staff phone screen - count occurrences of `pattern` in `str` as a
// subsequence, with the extra constraint that no two chosen indices in `str`
// may be adjacent.
//
// Build: g++ -std=c++17 -O0 -g nonadjacent.cpp && ./a.out

#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>

using namespace std;

// ---------------------------------------------------------------- your code

long long countMatches(const string& str, const string& pattern) {
    // TODO
    /*
    str[i] == pattern[j]:f(i,j) = f(i-1,j) + f(i-2,j-1)
否则:f(i,j) = f(i-1,j)
   f(i, j) means first i char of str match first j char of pattern, how much 
   f(0 ,0) =1 0 means null char
   f(i, 0) =1
   i < 2*j-1 =0
*/
    int n =str.size();
    int m =pattern.size();
    if (n < 2 * m -1) {
        return 0;
    }
    vector<vector<long long>> f(n+1, vector<long long>(m+1, 0));
    for (int i =0;i<=n;i++) {
        f[i][0] =1;
    }
    for (int i=1;i<=n;i++) {
        for (int j=1;j<=m;j++) {
            if (str[i-1] ==pattern[j-1]) {
                f[i][j] =f[i-1][j];
                if (i >=2) f[i][j] += f[i-2][j-1];
                if (i-2==-1 && i >= 2*j-1) f[i][j] +=1;
            }
            else {
                f[i][j] =f[i-1][j];
            }
        }
    }
    return f[n][m];
}

// ---------------------------------------------------------------- test harness

static int failures = 0;

static void expectEq(const string& s, const string& p, long long want) {
    long long got = countMatches(s, p);
    if (got != want) {
        cout << "FAIL  str=\"" << s << "\" pattern=\"" << p
             << "\"  got " << got << ", want " << want << "\n";
        ++failures;
    }
}

// brute force: enumerate every subset of indices of size |pattern|
static long long brute(const string& s, const string& p) {
    int n = (int)s.size(), m = (int)p.size();
    if (m == 0) return 1;
    if (n == 0) return 0;
    if (n > 22) { cout << "brute: string too long\n"; exit(1); }
    long long cnt = 0;
    for (long long mask = 0; mask < (1LL << n); ++mask) {
        if (__builtin_popcountll(mask) != m) continue;
        // adjacency check
        if (mask & (mask >> 1)) continue;
        // order-preserving match
        int k = 0;
        bool ok = true;
        for (int i = 0; i < n; ++i) {
            if (mask & (1LL << i)) {
                if (s[i] != p[k]) { ok = false; break; }
                ++k;
            }
        }
        if (ok && k == m) ++cnt;
    }
    return cnt;
}

int main() {
    // --- worked example from the problem statement
    expectEq("abab", "ab", 1);          // only indices (0,3)

    // --- single-character pattern: every occurrence counts, adjacency is vacuous
    expectEq("aaa",  "a",  3);
    expectEq("abc",  "d",  0);

    // --- empty pattern matches exactly once (the empty selection)
    expectEq("abc",  "",   1);
    expectEq("",     "",   1);

    // --- pattern longer than what the gap constraint allows
    expectEq("ab",   "ab", 0);          // (0,1) is adjacent
    expectEq("aba",  "aa", 1);          // (0,2) — checks the 2m-1 <= n boundary
    expectEq("a",    "aa", 0);

    // --- repeated characters, several legal selections
    expectEq("aabab", "ab", 3);
    expectEq("ababa", "aa", 3);         // (0,2) (0,4) (2,4)
    expectEq("aaaaa", "aa", 6);         // C(pairs with gap >= 2)

    // --- pattern not present at all
    expectEq("xyz",  "zy", 0);

    // --- randomized differential test against brute force
    srand(20260902);
    const char* alpha = "ab";
    for (int trial = 0; trial < 4000 && failures < 5; ++trial) {
        int n = rand() % 13;
        int m = rand() % 4;
        string s, p;
        for (int i = 0; i < n; ++i) s += alpha[rand() % 2];
        for (int i = 0; i < m; ++i) p += alpha[rand() % 2];
        long long got = countMatches(s, p);
        long long want = brute(s, p);
        if (got != want) {
            cout << "RANDOM FAIL  str=\"" << s << "\" pattern=\"" << p
                 << "\"  got " << got << ", want " << want << "\n";
            ++failures;
        }
    }

    // --- a slightly larger alphabet, longer strings
    const char* alpha3 = "abc";
    for (int trial = 0; trial < 3000 && failures < 5; ++trial) {
        int n = rand() % 18;
        int m = rand() % 5;
        string s, p;
        for (int i = 0; i < n; ++i) s += alpha3[rand() % 3];
        for (int i = 0; i < m; ++i) p += alpha3[rand() % 3];
        long long got = countMatches(s, p);
        long long want = brute(s, p);
        if (got != want) {
            cout << "RANDOM FAIL  str=\"" << s << "\" pattern=\"" << p
                 << "\"  got " << got << ", want " << want << "\n";
            ++failures;
        }
    }

    cout << (failures ? "FAILURES: " : "all passed, failures: ") << failures << "\n";
    return failures != 0;
}