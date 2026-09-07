#include <map>
#include <unordered_map>
#include <algorithm>
#include <string>
#include <cassert>

using namespace std;

class TimeKeyValueStore {
    unordered_map<string, map<int, string>> _cache;

public:
    void del(const string & key, int time) {
        _cache[key][time] ="###";
    }
    void set(const string & key, const string & value, int time) {
        _cache[key][time] =value;
    }

    string get(const string & key, int time) {
        if (_cache.count(key) ==0) {
            return "";
        }
        auto iter =_cache[key].upper_bound(time);
        if (iter ==_cache[key].begin()) {
            return "";
        }
        --iter;
        if (iter->second =="###") {
            return "";
        }
        return iter->second;
    }
    
};

int main () {
    TimeKeyValueStore store;
    string key ="doc1";
    string value ="draft-a";
    store.set(key, value, 5);
    assert (store.get("doc1", 5) == "draft-a");
    assert (store.get("doc1", 9) == "draft-a");    // → "draft-a"   (5 is the latest <= 9)
    assert (store.get("doc1", 3) == "");    // → ""          (nothing at or before 3)
    store.del("doc1", 8);
assert(store.get("doc1", 9) == "");
assert(store.get("doc1", 7) == "draft-a");
store.set("doc1", "draft-b", 12);
assert(store.get("doc1", 15) == "draft-b");
    return 0;
}