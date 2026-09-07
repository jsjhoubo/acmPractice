#include <bits/stdc++.h>
#include <queue>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS.  Streaming median, O(log n) add, O(1) query.
//    addNum(num): add a number to the stream
//    findMedian(): median of all so far (avg of middle two if even count)
// ============================================================
class MedianFinder {
private:
    priority_queue<int> _max_q;
    priority_queue<int, vector<int>, greater<int>> _min_q;
public:
    MedianFinder() {
        // TODO
    }
    void addNum(int num) {
        // TODO
        if (_max_q.size() ==0) {
            _max_q.push(num);
            return;
        }
        if (num>_max_q.top()) {
            _min_q.push(num);
        }
        else {
            _max_q.push(num);
        }
        
        while (_max_q.size() < _min_q.size()) {
            int x = _min_q.top();
            _max_q.push(x);
            _min_q.pop();
        }
        while (_max_q.size() > _min_q.size()+1) {
            int x = _max_q.top();
            _min_q.push(x);
            _max_q.pop();
        }
    }
    double findMedian() {
        // TODO
        
        size_t n =_max_q.size();
        size_t m =_min_q.size();
        if (n ==0) {
            return 0.0;
        }
        if ((m+n )%2==0) {
            return  (_max_q.top()*1.0 + _min_q.top()*1.0)/2;
        }

        return _max_q.top();
    }
};

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed=0, failed=0;
static void chk(const string& tag, double got, double exp) {
    if (fabs(got-exp) < 1e-9) { passed++; printf("  [PASS] %s = %g\n", tag.c_str(), got); }
    else { failed++; printf("  [FAIL] %s = %g, expected %g\n", tag.c_str(), got, exp); }
}

int main() {
    {
        // spec
        MedianFinder m;
        m.addNum(1); m.addNum(2);
        chk("spec_1.5", m.findMedian(), 1.5);
        m.addNum(3);
        chk("spec_2", m.findMedian(), 2.0);
    }
    {
        // single element
        MedianFinder m; m.addNum(42);
        chk("single", m.findMedian(), 42.0);
    }
    {
        // ascending insert
        MedianFinder m;
        for (int i=1;i<=5;i++) m.addNum(i);   // 1,2,3,4,5
        chk("asc_odd", m.findMedian(), 3.0);
        m.addNum(6);                          // 1..6
        chk("asc_even", m.findMedian(), 3.5);
    }
    {
        // descending insert (stresses rebalance the other direction)
        MedianFinder m;
        for (int i=5;i>=1;i--) m.addNum(i);   // inserted 5,4,3,2,1
        chk("desc_odd", m.findMedian(), 3.0);
    }
    {
        // negatives + mixed
        MedianFinder m;
        m.addNum(-1); m.addNum(-2); m.addNum(-3);
        chk("neg_odd", m.findMedian(), -2.0);
        m.addNum(0);
        chk("neg_even", m.findMedian(), -1.5);   // sorted: -3,-2,-1,0 -> (-2+-1)/2
    }
    {
        // duplicates
        MedianFinder m;
        m.addNum(5); m.addNum(5); m.addNum(5); m.addNum(5);
        chk("dups", m.findMedian(), 5.0);
    }
    {
        // interleaved, check running median at each step
        MedianFinder m;
        m.addNum(6);  chk("run1", m.findMedian(), 6.0);
        m.addNum(10); chk("run2", m.findMedian(), 8.0);
        m.addNum(2);  chk("run3", m.findMedian(), 6.0);   // 2,6,10
        m.addNum(6);  chk("run4", m.findMedian(), 6.0);   // 2,6,6,10 -> (6+6)/2
        m.addNum(5);  chk("run5", m.findMedian(), 6.0);   // 2,5,6,6,10
    }
    {
        // large-ish, values that would overflow int-average if summed naively
        MedianFinder m;
        m.addNum(2000000000); m.addNum(2000000000);
        chk("no_overflow_avg", m.findMedian(), 2000000000.0);  // avg of two equal
    }

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed==0 ? 0 : 1;
}