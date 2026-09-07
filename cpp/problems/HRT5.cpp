#include <cassert>
#include <iostream>
#include <string>

using namespace std;
bool valid(int val, bool flag) {
    return val >=0 && val <=255 && flag ==true;
}
bool isValidVersion(const string& s) {
    // TODO: implement
    int dot_count =0;
    int val=0;
    bool number_before =false;
    for (int i=0;i<s.size();i++) {
        if (s[i]=='.') {
            if (!valid(val, number_before)) {
                return false;
            }
            dot_count ++;
            if (dot_count >2) {
                return false;
            }
            val =0;
            number_before =false;
        }
        else if (s[i]>='0' && s[i] <='9') {
            val =val *10 + s[i]-'0';
            number_before =true;
        }
        else {
            return false;
        }
    }
    if (!valid(val, number_before)) {
        return false;
    }
    if (dot_count !=2) {
        return false;
    }
   
    return true;
}

int main() {
    // valid
    assert(isValidVersion("1.2.3") == true);
    assert(isValidVersion("10.0.25") == true);
    assert(isValidVersion("0.0.0") == true);
    assert(isValidVersion("255.255.255") == true);
    assert(isValidVersion("01.2.3") == true);

    // wrong number of parts
    assert(isValidVersion("1.2") == false);
    assert(isValidVersion("1.2.3.4") == false);

    // empty part
    assert(isValidVersion(".1.2") == false);
    assert(isValidVersion("1..2") == false);
    assert(isValidVersion("1.2.") == false);

    // non-digit characters
    assert(isValidVersion("1.a.3") == false);
    assert(isValidVersion("1.2.-3") == false);
    assert(isValidVersion("1.2.3x") == false);
    assert(isValidVersion(" 1.2.3") == false);
    assert(isValidVersion("1.2.3 ") == false);

    // out of range
    assert(isValidVersion("256.2.3") == false);
    assert(isValidVersion("1.999.3") == false);

    // edge cases
    assert(isValidVersion("") == false);
    assert(isValidVersion("...") == false);
    assert(isValidVersion("000.001.255") == true);

    cout << "All tests passed!\n";
    return 0;
}