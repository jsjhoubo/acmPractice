#include <bits/stdc++.h>
using namespace std;

class SourceTargetStr {
public:
    bool convertStr(const string& a, const string& b) {
        unordered_map<char, int> fre_a;
        unordered_map<char, int> fre_b;

        for (char ch : a) {
            fre_a[ch]++;
        }

        for (char ch : b) {
            fre_b[ch]++;
        }

        for (auto& [ch, cnt] : fre_b) {
            if (fre_a[ch] < cnt) {
                return false;
            }
        }

        return true;
    }

    int dp(const string& a, const string& b) {
        int m = a.size();
        int n = b.size();

        vector<int> f(n + 1, INT_MAX);
        f[0] = 0;

        for (int i = 0; i < n; i++) {
            int j = i + 1;

            for (int k = 1; k <= m && k <= j; k++) {
                string sub = b.substr(j - k, k);

                if (convertStr(a, sub) && f[j - k] != INT_MAX) {
                    f[j] = min(f[j], f[j - k] + 1);
                }
            }
        }

        return f[n] == INT_MAX ? -1 : f[n];
    }
};

int main() {
    SourceTargetStr solver;

    vector<pair<pair<string, string>, int>> tests = {
        {{"ap", "papa"}, 2},
        {{"abc", "abccba"}, 2},
        {{"abc", "abcd"}, -1},
        {{"ab", "aaa"}, 3},
        {{"aab", "aaaa"}, 2},
        {{"aab", "aaaab"}, 3},
        {{"xyz", ""}, 0},
        {{"abc", "cababc"}, 2}
    };

    for (int i = 0; i < (int)tests.size(); i++) {
        string source = tests[i].first.first;
        string target = tests[i].first.second;
        int expected = tests[i].second;

        int got = solver.dp(source, target);

        cout << "Test " << i + 1 << ": ";
        cout << "source = \"" << source << "\", ";
        cout << "target = \"" << target << "\"\n";

        cout << "Expected: " << expected << ", ";
        cout << "Got: " << got << " ";

        if (got == expected) {
            cout << "[PASS]";
        } else {
            cout << "[FAIL]";
        }

        cout << "\n\n";
    }

    return 0;
}