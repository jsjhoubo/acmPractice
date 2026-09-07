// Coupang - basic calculator.
// Evaluate a string expression with non-negative integers, + - * /,
// parentheses and spaces. Integer division truncates toward zero.
// The expression is guaranteed to be well-formed.
//
// Build: g++ -std=c++17 -O0 -g calculator.cpp && ./a.out

#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cctype>
#include <stack>

using namespace std;

// ---------------------------------------------------------------- your code
/*
例:"3+2*2" → 7,"(1+(4+5+2)-3)+(6+8)" → 23," 3/2 " → 1,"2*(5+5*2)/3+(6/2+8)" → 21
*/
/*
3+2*(5+5)/2

 3 + 10
     2*(5+5)/2 =>2*5=10
       (5+5)/2 =》 10/2
        5 + 5

3 + 10 + 2

  dfs(s, &start):
    stack<int> st;
    while (Token is not null) {
        if token is number or (:
          number =parse token or dfs(s, start)
          if lastop is null
            st.puhs(number )
          else
            if last op =-
                st push -number
            else if last op +
                st. push number
            else if last op ='* /'
                x = s top
                st.pop
                st.push(x * / number)
            lastop =null             
        if token is op (+/-* /)
          lastop =token
        if token is )
           break
           
    }

*/

int dfs(const string & s, int &i) {
    stack<int> st;
    char last_op =0;
    while (i < s.size()) {
        if ((s[i]>='0' && s[i]<='9') ||s[i]=='(') {
            int number =0;
            if (s[i] =='(') {
                i++;
                number =dfs(s, i);
            }
            else {
                while (i <s.size() && (s[i]>='0' && s[i]<='9')) {
                    number = number *10 + s[i] -'0';
                    i++;
                }
            }
            if (last_op ==0) {
                st.push(number);
            }
            else {
                if (last_op =='-')
                    st.push(-number);
                else if (last_op =='+') {
                    st.push(number);
                }
                else {
                    int x = st.top();
                    st.pop();
                    if (last_op =='*')
                        st.push(x * number);
                    else if (last_op=='/') {
                        st.push(x/number);
                    }
                }
                last_op =0;
            }
        }  
        else if (s[i]==')') {
            i++;
            break;
        }
        else if (s[i] ==' ') {
            i++;
            continue;
        }
        else {
            last_op =s[i];
            i++;
        }
    }
    int ret =0;
    while (!st.empty()) {
        ret += st.top();
        st.pop();
    }
    return ret;
}

int calculate(const string& s) {
    // TODO
    int i =0;
    int ret =dfs(s, i);
    return ret;
}

// ---------------------------------------------------------------- test harness

static int failures = 0;

static void expectEq(const string& expr, int want) {
    int got = calculate(expr);
    if (got != want) {
        cout << "FAIL  \"" << expr << "\"  got " << got << ", want " << want << "\n";
        ++failures;
    }
}

// -------- independent reference: recursive-descent parser over the grammar
//   expr   := term (('+'|'-') term)*
//   term   := factor (('*'|'/') factor)*
//   factor := number | '(' expr ')'
// Written top-down so it shares no structure with a stack-based solution.
struct Parser {
    const string& s;
    size_t i = 0;
    explicit Parser(const string& str) : s(str) {}
    void skip() { while (i < s.size() && s[i] == ' ') ++i; }
    long long expr() {
        long long v = term();
        for (;;) {
            skip();
            if (i < s.size() && s[i] == '+') { ++i; v += term(); }
            else if (i < s.size() && s[i] == '-') { ++i; v -= term(); }
            else return v;
        }
    }
    long long term() {
        long long v = factor();
        for (;;) {
            skip();
            if (i < s.size() && s[i] == '*') { ++i; v *= factor(); }
            else if (i < s.size() && s[i] == '/') { ++i; long long d = factor(); v /= d; }
            else return v;
        }
    }
    long long factor() {
        skip();
        if (i < s.size() && s[i] == '(') {
            ++i;
            long long v = expr();
            skip();
            ++i;                       // consume ')'
            return v;
        }
        long long v = 0;
        while (i < s.size() && isdigit((unsigned char)s[i])) { v = v * 10 + (s[i] - '0'); ++i; }
        return v;
    }
};

static int refEval(const string& s) { Parser p(s); return (int)p.expr(); }

// random well-formed expression generator (non-negative ints, no div-by-zero)
static string gen(int depth, int& guard) {
    if (--guard <= 0) return to_string(rand() % 9 + 1);
    if (depth <= 0 || rand() % 3 == 0) return to_string(rand() % 20);
    string a = gen(depth - 1, guard);
    string b = gen(depth - 1, guard);
    const char ops[] = {'+', '-', '*', '/'};
    char op = ops[rand() % 4];
    if (op == '/') b = "(" + b + "+1)";          // avoid division by zero
    string joined = a + string(1, op) + b;
    if (rand() % 2) joined = "(" + joined + ")";
    if (rand() % 3 == 0) joined = " " + joined + " ";
    return joined;
}

int main() {
    // --- single number, whitespace
    expectEq("0", 0);
    expectEq("42", 42);
    expectEq("   7   ", 7);

    // --- flat addition and subtraction
    expectEq("1+1", 2);
    expectEq("2-1+2", 3);
    expectEq(" 2-1 + 2 ", 3);

    // --- precedence
    expectEq("3+2*2", 7);
    expectEq("2*3+4", 10);
    expectEq("3+5/2", 5);

    // --- division truncation
    expectEq("3/2", 1);
    expectEq(" 3/2 ", 1);
    expectEq("1/2", 0);
    expectEq("14-3/2", 13);

    // --- parentheses
    expectEq("(1+1)", 2);
    expectEq("2*(5+5)", 20);
    expectEq("(1+(4+5+2)-3)+(6+8)", 23);
    expectEq("((((5))))", 5);
    expectEq("2*(5+5*2)/3+(6/2+8)", 21);

    // --- parentheses interacting with precedence
    expectEq("(2+6*3+5-(3*14/7+2)*5)+3", -12);
    expectEq("1-(5-2)", -2);
    expectEq("10/(2+3)", 2);
    expectEq("100/((2+3)*5)", 4);

    // --- long chains
    expectEq("1+2+3+4+5+6+7+8+9+10", 55);
    expectEq("2*2*2*2*2", 32);
    expectEq("1000/2/2/2/5", 25);

    // --- randomized differential test against the recursive-descent reference
    srand(20260902);
    for (int trial = 0; trial < 4000 && failures < 6; ++trial) {
        int guard = 14;
        string e = gen(3, guard);
        int got = calculate(e);
        int want = refEval(e);
        if (got != want) {
            cout << "RANDOM FAIL  \"" << e << "\"  got " << got << ", want " << want << "\n";
            ++failures;
        }
    }

    cout << (failures ? "FAILURES: " : "all passed, failures: ") << failures << "\n";
    return failures != 0;
}