#include <string>
#include <unordered_map>
#include <iostream>
using namespace std;
int lengthOfLongestSubstring(string s) {
    // your code here
    // this used window scanning method
    int l =0;
    int r =0;
    unordered_map<char, int> fre;
    // l ..r-1 is the valid window
    int n =s.size();
    int ret =0;
    while (l <n) {
        while (r < n) {
            char ch = s[r];
            if (fre[ch] ==0) {
                ++fre[ch];
                ++r;
            }
            else {
                break;
            }
        }
        ret =max(ret, r -l);
        --fre[s[l]];
        l++;
    }
    return ret;
}


int main() {
    // test cases
    cout << lengthOfLongestSubstring("abcabcbb") << endl;  // expected: 3
    cout << lengthOfLongestSubstring("bbbbb") << endl;     // expected: 1
    cout << lengthOfLongestSubstring("pwwkew") << endl;    // expected: 3
    cout << lengthOfLongestSubstring("") << endl;          // expected: 0
    return 0;
}