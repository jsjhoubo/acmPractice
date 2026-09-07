#include <vector>
#include <stack>
#include <functional>
#include <climits>
#include <bits/stdc++.h>
using namespace std;

// ============================================================
//  Provided interface (test implementation).
//  A NestedInteger is EITHER a single int OR a list.
// ============================================================
class NestedInteger {
    bool _isInt;
    int _val;
    vector<NestedInteger> _list;
public:
    NestedInteger(int v) : _isInt(true), _val(v) {}
    NestedInteger(vector<NestedInteger> l) : _isInt(false), _val(0), _list(move(l)) {}
    bool isInteger() const { return _isInt; }
    int getInteger() const { return _val; }
    const vector<NestedInteger>& getList() const { return _list; }
};
// convenience builders
static NestedInteger I(int v){ return NestedInteger(v); }
static NestedInteger L(vector<NestedInteger> v){ return NestedInteger(move(v)); }

// ============================================================
//  YOU IMPLEMENT THIS.
// ============================================================
class NestedIterator {
private:
   vector<NestedInteger> _nestedList;
   stack<pair<vector<NestedInteger>, int>> _st;
   bool _hasNext;
   int _cur;
   void advance() {
        while (_st.size() >0) {
            auto & [l, ind] = _st.top();
            if (ind == l.size()) {
                _st.pop();
            }
            else {
                if (l[ind].isInteger()) {
                    _cur =l[ind].getInteger();
                    _hasNext =true;
                }
                else {
                    _st.push({l[ind].getList(), 0});
                }
                ind += 1;
                if(_hasNext) {
                    return;
                }
            }
        }
        _hasNext =false;
        
    }
public:
    NestedIterator(const vector<NestedInteger>& nestedList) {
        _nestedList = nestedList;
        _st.push({_nestedList, 0});
        _hasNext =false;
    }
    
    int next() {
        if (!_hasNext) {
            advance();
        }
        _hasNext =false;
        return _cur;
    }
    bool hasNext() {
        if (!_hasNext) {
            advance();
        }
        return _hasNext;
    }
};

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed=0, failed=0;
static void check(const string& name, const vector<NestedInteger>& input,
                  const vector<int>& expected) {
    NestedIterator it(input);
    vector<int> got;
    // call hasNext() twice each iteration to test idempotency
    while (it.hasNext()) {
        bool again = it.hasNext();      // must not advance
        (void)again;
        got.push_back(it.next());
    }
    // extra hasNext after exhaustion must be false
    bool tail = it.hasNext();
    if (got == expected && !tail) {
        passed++;
        printf("  [PASS] %s -> [", name.c_str());
        for (size_t i=0;i<got.size();i++) printf("%d%s", got[i], i+1<got.size()?",":"");
        printf("]\n");
    } else {
        failed++;
        printf("  [FAIL] %s\n         got      [", name.c_str());
        for (size_t i=0;i<got.size();i++) printf("%d%s", got[i], i+1<got.size()?",":"");
        printf("]  (tail hasNext=%s)\n         expected [", tail?"true":"false");
        for (size_t i=0;i<expected.size();i++) printf("%d%s", expected[i], i+1<expected.size()?",":"");
        printf("]\n");
    }
}

int main() {
    // 1) spec: [[1,1],2,[1,1]] -> 1,1,2,1,1
    check("nested_mix",
          { L({I(1),I(1)}), I(2), L({I(1),I(1)}) },
          {1,1,2,1,1});

    // 2) deep: [1,[4,[6]]] -> 1,4,6
    check("deep",
          { I(1), L({ I(4), L({ I(6) }) }) },
          {1,4,6});

    // 3) empty list at top: [] -> nothing
    check("empty_top", {}, {});

    // 4) list of empties: [[],[]] -> nothing
    check("empty_nested",
          { L({}), L({}) },
          {});

    // 5) [[]] -> nothing
    check("single_empty",
          { L({}) },
          {});

    // 6) empties interleaved with ints: [[],1,[],2,[[]],3]
    check("empties_interleaved",
          { L({}), I(1), L({}), I(2), L({ L({}) }), I(3) },
          {1,2,3});

    // 7) single int: [5]
    check("single_int", { I(5) }, {5});

    // 8) deeply nested single: [[[[7]]]]
    check("deep_single",
          { L({ L({ L({ I(7) }) }) }) },
          {7});

    // 9) INT_MIN as legit data (sentinel trap)
    check("intmin_data",
          { I(INT_MIN), I(0), I(INT_MIN) },
          {INT_MIN, 0, INT_MIN});

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed==0 ? 0 : 1;
}