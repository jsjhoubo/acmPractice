#include <cassert>
#include <cmath>
#include <iostream>
#include <queue>
using namespace std;

class MovingAverage {
    int _n;
    double _sum ;
    queue<int> _qu;
public:
// state
// q :contains latest at most windowsSize numbers
// sum: sum of all numbers in the q
// Invariant
// Add:
//   sum += x
//   push x to qu
//   if size of qu > n
//       sum -= qu.front
//       qu.pop
//   return sum/qu.size
    MovingAverage(int windowSize) {
        _n =windowSize;
        _sum =0;
    }

    double add(int x) {
        // TODO
        _sum += x;
        _qu.push(x);
        
        if (_qu.size() >_n) {
            _sum -= _qu.front();
            _qu.pop();
        }

        return _sum/_qu.size();
    }
};

bool eq(double a, double b) {
    return fabs(a - b) < 1e-9;
}

int main() {
    {
        MovingAverage m(3);

        assert(eq(m.add(1), 1.0));
        assert(eq(m.add(10), 5.5));
        assert(eq(m.add(3), 14.0 / 3));
        assert(eq(m.add(5), 6.0));      // [10,3,5]
        assert(eq(m.add(-2), 2.0));     // [3,5,-2]
    }

    {
        MovingAverage m(1);

        assert(eq(m.add(7), 7.0));
        assert(eq(m.add(100), 100.0));
        assert(eq(m.add(-3), -3.0));
    }

    {
        MovingAverage m(4);

        assert(eq(m.add(0), 0.0));
        assert(eq(m.add(0), 0.0));
        assert(eq(m.add(8), 8.0 / 3));
        assert(eq(m.add(4), 3.0));
        assert(eq(m.add(4), 4.0));      // [0,8,4,4]
    }

    cout << "All tests passed!\n";
}