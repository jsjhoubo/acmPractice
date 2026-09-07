#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace std;



bool isDigit(char ch) {
    return ch >='0' && ch<='9';
}
bool isCapitalLetter(char ch) {
    return (ch >='A' && ch<='Z');
}
bool isSmallLetter(char ch) {
    return (ch >='a' && ch<='z');
}
void parseString(const string & str, unordered_map<string, int> & left, unordered_map<string, int> & right) {
    int i=0;
    bool is_left =true;
    int cur_number =1;
    while (i<str.size()) {
        if (str[i]=='=') {
            is_left =false;
            cur_number =1;
            i++;
        }
        else if (isDigit(str[i])) {
            int val =0;
            while (i <str.size() && isDigit(str[i])) {
                val =val * 10 + str[i]-'0';
                i++;
            }
            cur_number =val;
        }
        else if (isCapitalLetter(str[i])) {
            int j =i;
            i++;
            while (i<str.size() && isSmallLetter(str[i])) {
                i++;
            }
            string cur_element =str.substr(j, i-j);
            if (i == str.size()) {
                break;
            }
            if (isDigit(str[i])) {
                int val =0;
                while (i <str.size() && isDigit(str[i])) {
                    val =val * 10 + str[i]-'0';
                    i++;
                }
            
            
                if (is_left) {
                    left[cur_element] += cur_number *val;
                }
                else {
                    right[cur_element] += cur_number *val;
                }
            }
            else {
                if (is_left) {
                    left[cur_element] += cur_number;
                }
                else {
                    right[cur_element] += cur_number;
                }
            }
        }
        else if (str[i] =='+') {
            cur_number =1;
            i++;
        }
        else {
            i++; // space or others
        }
    }
}


bool isBalanced(const string& s) {
    // TODO: implement
    unordered_map<string, int> left, right;
    parseString(s, left, right);
    if (left.size()!=right.size()) {
        return false;
    }
    for (auto & [key, c] : left) {
        if (right.count(key) ==0) {
            return false;
        }
        if (right[key] !=c)  {
            return false;
        }
    }
    return true;
}

int main() {
    assert(isBalanced("2H2 + O2 = 2H2O") == true);

    assert(isBalanced("1000H2O = Au + Ag") == false);

    assert(isBalanced("H2 = H2") == true);

    assert(isBalanced("NaCl = NaCl") == true);

    assert(isBalanced("2NaCl = Na2Cl2") == true);

    assert(isBalanced("H2 + O = H2O") == true);

    assert(isBalanced("H2 + O2 = H2O") == false);

    assert(isBalanced("FF = F2") == true);

    assert(isBalanced("3H2O = H6O3") == true);

    assert(isBalanced("2Abc3 + X = Abc6 + X") == true);

    cout << "All tests passed!\n";

    return 0;
}