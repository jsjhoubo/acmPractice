#include <bits/stdc++.h>
#include <deque>
#include <unordered_map>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS CLASS.
//  Sliding window of the last w ticks. Amortized O(1) push/max/min.
//  Assume max()/min() are only called after at least one push.
// ============================================================
class TickWindow {
private:
    deque<size_t> _inc_q;
    deque<size_t> _dec_q;
    unordered_map<size_t, double> _cache;
    size_t _w;
    size_t _cur;
public:
    TickWindow(int w) {
        // TODO
        _w =w;
        _cur =0;
    }
    void push(double price) {
        // TODO
        _cache[_cur] =price;
        while (_inc_q.size()>0 && _cache[_inc_q.back()] > price){
            _inc_q.pop_back();
        }
        _inc_q.push_back(_cur);

        while(_dec_q.size()>0 && _cache[_dec_q.back()] < price) {
            _dec_q.pop_back();
        }
        _dec_q.push_back(_cur);
        
        if (_cache.size() > _w) {
            size_t index =_cur -_w;
            _cache.erase(index);
            while (!_inc_q.empty() && _inc_q.front() <= index) {
                _inc_q.pop_front();
            }
            while (!_dec_q.empty() && _dec_q.front() <= index) {
                _dec_q.pop_front();
            }
        }
        ++_cur;
    }
    double max() {
        // TODO
        if (_dec_q.size() >0) {
            return _cache[_dec_q.front()];
        }
        return 0;
    }
    double min() {
        // TODO
        if (_inc_q.size() >0) {
            return _cache[_inc_q.front()];
        }
        return 0;
    }
};

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed = 0, failed = 0;
static void chk(const string& tag, double got, double exp) {
    if (fabs(got - exp) < 1e-9) { passed++; printf("  [PASS] %s = %g\n", tag.c_str(), got); }
    else { failed++; printf("  [FAIL] %s got %g, expected %g\n", tag.c_str(), got, exp); }
}

int main() {
    {
        // spec walkthrough, w=3
        TickWindow tw(3);
        tw.push(10); chk("t1.max",tw.max(),10); chk("t1.min",tw.min(),10);
        tw.push(8);  chk("t2.max",tw.max(),10); chk("t2.min",tw.min(),8);
        tw.push(12); chk("t3.max",tw.max(),12); chk("t3.min",tw.min(),8);
        tw.push(5);  chk("t4.max",tw.max(),12); chk("t4.min",tw.min(),5);   // 10 slides out
        tw.push(5);  chk("t5.max",tw.max(),12); chk("t5.min",tw.min(),5);   // window [12,5,5]
        tw.push(5);  chk("t6.max",tw.max(),5);  chk("t6.min",tw.min(),5);   // 12 slides out -> [5,5,5]
    }
    {
        // w=1 : window is always just the latest tick
        TickWindow tw(1);
        tw.push(7);  chk("w1.a.max",tw.max(),7); chk("w1.a.min",tw.min(),7);
        tw.push(3);  chk("w1.b.max",tw.max(),3); chk("w1.b.min",tw.min(),3);
        tw.push(9);  chk("w1.c.max",tw.max(),9); chk("w1.c.min",tw.min(),9);
    }
    {
        // strictly increasing prices, w=3
        TickWindow tw(3);
        tw.push(1); tw.push(2); tw.push(3);
        chk("inc.max",tw.max(),3); chk("inc.min",tw.min(),1);
        tw.push(4);  // window [2,3,4]
        chk("inc2.max",tw.max(),4); chk("inc2.min",tw.min(),2);
    }
    {
        // strictly decreasing prices, w=2
        TickWindow tw(2);
        tw.push(9); tw.push(7);
        chk("dec.max",tw.max(),9); chk("dec.min",tw.min(),7);
        tw.push(5);  // window [7,5]
        chk("dec2.max",tw.max(),7); chk("dec2.min",tw.min(),5);
    }
    {
        // duplicates / plateau, w=3  — tests index-based expiry, not value
        TickWindow tw(3);
        tw.push(4); tw.push(4); tw.push(4);
        chk("dup.max",tw.max(),4); chk("dup.min",tw.min(),4);
        tw.push(4);  // still all 4s
        chk("dup2.max",tw.max(),4); chk("dup2.min",tw.min(),4);
    }
    {
        // window not yet full behaves as window over all-so-far
        TickWindow tw(5);
        tw.push(3); chk("nf.a.max",tw.max(),3); chk("nf.a.min",tw.min(),3);
        tw.push(1); chk("nf.b.max",tw.max(),3); chk("nf.b.min",tw.min(),1);
        tw.push(2); chk("nf.c.max",tw.max(),3); chk("nf.c.min",tw.min(),1);
    }
    {
        // the classic trap: max element expires, second-highest must surface
        TickWindow tw(3);
        tw.push(100); tw.push(1); tw.push(2);
        chk("trap.max",tw.max(),100);
        tw.push(3);  // window [1,2,3], the 100 expired
        chk("trap2.max",tw.max(),3); chk("trap2.min",tw.min(),1);
    }

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed == 0 ? 0 : 1;
}