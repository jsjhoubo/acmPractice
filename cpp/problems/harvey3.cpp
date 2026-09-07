#include <map>
#include <vector>
#include <algorithm>
#include <set>
#include <unordered_map>
#include <string>
#include <assert.h>
#include <queue>
using namespace std;

class RateLimiter {
    size_t _limit;
    int _window;
    unordered_map<string, deque<int>> _cache;
public:
    RateLimiter(size_t limit, int window) :_limit(limit), _window(window) {}

    bool Allow(const string & firm, int time) {
        _cache[firm].push_back(time);
        auto & data =_cache[firm]; 
        while(data.empty() ==false  && data.front() <= time -_window) {
            data.pop_front();
        }
        if (data.size() <= _limit) {
            return true;
        }
        data.pop_back();
        return false;
    }


};

int main() {
    RateLimiter rl(3, 10);
    assert(rl.Allow("acme", 1) == true );
    assert(rl.Allow("acme", 2) == true );
    assert(rl.Allow("acme", 3) == true );
    assert(rl.Allow("acme", 4) == false);
    assert(rl.Allow("acme", 11) == true );
    assert(rl.Allow("beta", 4) == true );
}