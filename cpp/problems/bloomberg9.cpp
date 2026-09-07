#include <bits/stdc++.h>
#include <stack>
#include <string>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS.
//  Evaluate expression: non-negative ints, + - * / , spaces.
//  * and / have higher precedence than + and -.
//  / truncates toward zero. No parentheses.
// ============================================================
int calculate(const string& str) {
    // TODO: your code here
    stack<int> num_st;
    stack<string> op_st;
    //2 + 3*5
    // 2 3
    // + 
    int cur =0;
    auto nextToken =[&]() ->string {
        while (cur < str.size() && str[cur] ==' ') {
          ++cur;
        }
        int i =cur;
        if (cur <str.size() && (str[cur]=='+' || str[cur]=='-' || str[cur]=='*' || str[cur]=='/')) {
            ++cur;
            return string(str, i, 1);
        }
        while (cur<str.size() && str[cur] >='0' && str[cur] <='9') {
            ++cur;
        }
        return str.substr(i, cur -i);
    };
    while (true) {
        string s =nextToken();
        if (s.size() ==0) {
            break;
        }
        if (s =="+" || s =="-" ) {
            op_st.push(s);
        }
        else if (s=="*" || s=="/") {
            string s1 =nextToken();
            int x =num_st.top();
            int y =stoi(s1);
            num_st.pop();
            int z =0;
            if (s =="*") {
                z =x *y;
            }
            else {
                z =x/y;
            }
            num_st.push(z);
        }
        else {
            num_st.push(stoi(s));
        }
    }
    stack<string> op_st1;
    stack<int> num_st1;
    while (num_st.size() >0) {
        num_st1.push(num_st.top());
        num_st.pop();
    }
    while (op_st.size() >0) {
        op_st1.push(op_st.top());
        op_st.pop();
    }
    while (op_st1.size() >0) {
        string op =op_st1.top();
        op_st1.pop();
        int x =num_st1.top();
        num_st1.pop();
        int y =num_st1.top();
        num_st1.pop();
        if (op=="+" ) {
            num_st1.push(x+y);
        }
        else {
            num_st1.push(x-y);
        }
    }
    return num_st1.top();
}

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed=0, failed=0;
static void chk(const string& s, int expected) {
    int got = calculate(s);
    if (got == expected) { passed++; printf("  [PASS] \"%s\" = %d\n", s.c_str(), got); }
    else { failed++; printf("  [FAIL] \"%s\" = %d, expected %d\n", s.c_str(), got, expected); }
}

int main() {
    chk("3+2*2", 7);
    chk(" 3/2 ", 1);
    chk("3+5 / 2", 5);
    chk("14-3/2", 13);
    chk("42", 42);
    chk(" 42 ", 42);
    chk("1+1+1", 3);
    chk("2*3*4", 24);
    chk("100/10/2", 5);
    chk("1-1+1", 1);           // left-to-right for same precedence
    chk("0", 0);
    chk("10+2*3-4/2", 14);     // 10+6-2 = 14
    chk("7-3-2", 2);           // 7-3-2 = 2 (left assoc)
    chk("2*3+4*5", 26);        // 6+20
    chk("1000000+1000000", 2000000);
    chk("14-3*2", 8);          // 14-6
    chk("3+2*2-1", 6);
    chk("6/4", 1);             // truncation toward zero
    chk("0*5+3", 3);
    chk("5+0*100", 5);

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed==0 ? 0 : 1;
}