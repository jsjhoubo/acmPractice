#include <bits/stdc++.h>
#include <algorithm>
using namespace std;

// TODO: implement this function
int lengthOfLongestSubstring(const string& s) {
    // your code here
    int c[26];
    for (int i=0;i<26;i++) {
        c[i] =0;
    }
    int ret =0;
    size_t j =0;
    for (size_t i=0;i<s.size();i++) {
        size_t idx =s[i] -'a';
        c[idx] ++;
        while (c[idx] >1) {
            c[s[j]-'a']--;
            j++;
        }
        ret =max(ret, (int)(i-j+1));
    }
    return ret;
}

int main() {   
    vector<string> tests = {
        "abcabcbb",
        "bbbbb",
        "pwwkew",
        "",
        "a",
        "abba",
        "dvdf",
        "anviaj"
    };

    vector<int> expected = {
        3,
        1,
        3,
        0,
        1,
        2,
        3,
        4
    };

    for (size_t i = 0; i < tests.size(); ++i) {
        int got = lengthOfLongestSubstring(tests[i]);
        cout << "Test " << i + 1 << ": ";
        cout << "s = \"" << tests[i] << "\"";
        cout << ", expected = " << expected[i];
        cout << ", got = " << got;

        if (got == expected[i]) {
            cout << "  [PASS]";
        } else {
            cout << "  [FAIL]";
        }
        cout << endl;
    }

    return 0;
}