#include <algorithm>
#include <cassert>
#include <iostream>
#include <utility>
#include <vector>

using namespace std;

class SessionCounter {
private:
    vector<pair<int, int>> _intervals;

public:
    SessionCounter(const vector<pair<int, int>>& intervals) {
        // TODO:
        // preprocess intervals
        _intervals =intervals;
        sort(_intervals.begin(), _intervals.end(), [](const pair<int, int> & a, const pair<int, int> & b){
            if (a.first != b.first) {
                return a.first <b.first;
            }
            return a.second < b.second;
        });
    }

    // we need to find the smallest start which greater than t, assume it is key
    // then from  key-1, until 0, find correct answer
    int countActive(int t) const {
        // TODO:
        // return number of intervals satisfying:
        // start <= t && t < end
        int l =0;
        int r =_intervals.size()-1;
        int key =r+1;
        while (l <=r) {
            int mid = l + (r -l)/2;
            if (_intervals[mid].first > t) {
                r =mid-1;
                key =mid;
            }
            else {
                l =mid+1;
            }
        }
        int n =_intervals.size();
        
        int cnt =0;
        for (int i =key-1;i>=0;i--) {
            if (_intervals[i].first <=t && t< _intervals[i].second ) {
                cnt ++;
            }
        }
        return cnt;
    }
};

int main() {
    vector<pair<int, int>> intervals = {
        {1, 5},
        {2, 7},
        {4, 6},
        {7, 10},
        {7, 8}
    };

    SessionCounter counter(intervals);

    assert(counter.countActive(0) == 0);
    assert(counter.countActive(1) == 1);
    assert(counter.countActive(2) == 2);
    assert(counter.countActive(4) == 3);

    // [1,5) has already ended
    assert(counter.countActive(5) == 2);

    assert(counter.countActive(6) == 1);

    // [2,7) ends exactly at 7,
    // while [7,10) and [7,8) start exactly at 7
    assert(counter.countActive(7) == 2);

    assert(counter.countActive(8) == 1);
    assert(counter.countActive(9) == 1);
    assert(counter.countActive(10) == 0);
    assert(counter.countActive(100) == 0);

    cout << "All tests passed!\n";
    return 0;
}