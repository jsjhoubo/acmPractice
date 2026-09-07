#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace std;


/*
state:
 hash map, dict: char->count
 cnt total coutn
algorithm:
  scan strint get dict

  for ch in a-z:
    dict update
       if string +1 is even: 
         check dict number of odds count, if any count =1 break else cnt
       if string +1 is odd:
         check dict number of odds count and even count, if odds count =1 cnt ++ else break
    dict update
   
*/

/*
state:
 hash map, dict: char->fre
 
 odd_cnt number of character which fre is odd
algorithm:
  scan strint get dict
  scan dict get odd_cnt 
  if string is even, odd_cnt=0, +26
                     odd_cnt =2, 2 ab=> aba, bab
                     odd_cnt >2 abcd 0
  if string is odd, odd_count !=0
                     odd_ccnt =1 , aabbc, 1
                     odd_cnt >1 aabbcde, 0
   
*/
int countWays(const string& s) {
    // TODO

    unordered_map<char, int> dict;
    int cnt =0;
    int odd_cnt =0;
    for (auto ch : s) {
        dict[ch]++;
    }
    for (auto & [ch, fre] : dict) {
        if (fre % 2 ==1) {
            odd_cnt++;
        }
    }
    
    if (s.size()%2 ==0) {
        if (odd_cnt ==0) {
            return 26;
        }
        if (odd_cnt ==2) {
            return 2;
        }
    }
    else {
        if (odd_cnt ==1) {
            return 1;
        }
    }
    return 0;
}

int main() {
    assert(countWays("") == 26);

    assert(countWays("a") == 1);

    assert(countWays("ab") == 2);
    // add a -> aab
    // add b -> abb

    assert(countWays("abc") == 0);

    assert(countWays("aa") == 26);

    assert(countWays("aab") == 1);
    // only add b -> aabb

    assert(countWays("aabb") == 26);

    cout << "All tests passed!\n";
}