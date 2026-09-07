#include <bits/stdc++.h>
#include <assert.h>

using namespace std;

// LFU Cache (LC 460)
// 结构: freq -> 该频次的桶(桶内 LRU 序), 外加 key -> {value, freq, 桶内位置}
// 满时驱逐: 最小频次桶的 LRU 端; 频次相同驱逐最久未访问
// get/put 均 O(1)
class LFUCache {
    int _capacity;
    unordered_map<int, pair<size_t, int>> _cache;
    map<size_t, unordered_map<int, list<int>::iterator>> _freMap;
    map<size_t, list<int>> _link;

    void updateCache(int key) {
        if (_cache.count(key)==0) {
            return;
        }
        size_t fre =_cache[key].first;
        if (_freMap.count(fre) ==0) {
            return;
        }
        auto & hash = _freMap[fre];
        if (hash.count(key) ==0) {
            return;
        } 
        auto p = hash[key];
        _freMap[fre].erase(key);
        _link[fre].erase(p);
        if (_freMap[fre].size()==0) {
            _freMap.erase(fre);
            _link.erase(fre);
        }
        
        _link[fre+1].push_front(key);
        _freMap[fre+1][key] =_link[fre+1].begin();
        _cache[key].first++;
    }

    void deleteItem() {
        if (_cache.size() ==_capacity) {
            auto last = _link.begin();
            int key = last->second.back();
            _cache.erase(key);
            size_t fre =last->first;
            _freMap[fre].erase(key);
            _link[fre].pop_back();
            if (_link[fre].size()==0) {
                _link.erase(fre);
                _freMap.erase(fre);
            }
        }
    }
public:
    LFUCache(int capacity) :_capacity(capacity){
        assert(capacity >0);
    }

    // 存在返回 value(算一次访问, freq+1), 否则返回 -1
    int get(int key) {
        if (_cache.count(key) ==0) {
            return -1;
        }
        updateCache(key);
        return _cache[key].second;
    }

    // 已存在: 更新 value, 算一次访问(freq+1)
    // 新 key: 满则先驱逐, 以 freq=1 插入
    // capacity == 0: 所有 put 无效
    void put(int key, int value) {
        if (_cache.count(key) >0) {
            _cache[key].second =value;    
            updateCache(key);
        }
        else {
            deleteItem();
            _cache[key] ={1, value};
            _link[1].push_front(key);
            _freMap[1][key] =_link[1].begin();
        }
        
    }

private:
    // 你来填
};

// ---------------- Testing ----------------
int main() {
    // Test 1: LC 460 官方样例
    {
        LFUCache c(2);
        c.put(1, 1);
        c.put(2, 2);
        assert(c.get(1) == 1);      // freq(1)=2, freq(2)=1
        c.put(3, 3);                // 驱逐 2(freq 最低)
        assert(c.get(2) == -1);
        assert(c.get(3) == 3);      // freq(3)=2
        c.put(4, 4);                // 1 和 3 频次同为 2, 驱逐更久未访问的 1
        assert(c.get(1) == -1);
        assert(c.get(3) == 3);
        assert(c.get(4) == 4);
    }

    // Test 2: 频次平局时严格按桶内 LRU(get 也刷新桶内位置)
    {
        LFUCache c(3);
        c.put(1, 10); c.put(2, 20); c.put(3, 30);   // 全在 freq=1
        c.get(1); c.get(2); c.get(3);               // 全升到 freq=2, 桶内序: 3,2,1
        c.get(1);                                    // 1 升到 freq=3
        c.put(4, 40);                                // freq=2 桶驱逐 LRU 端 = 2
        assert(c.get(2) == -1);
        assert(c.get(3) == 30);
        assert(c.get(1) == 10);
        assert(c.get(4) == 40);
    }

    // Test 3: put 覆盖也算访问(freq+1), 且不触发驱逐
    {
        LFUCache c(2);
        c.put(1, 1); c.put(2, 2);
        c.put(1, 11);               // 覆盖: freq(1)=2, 不驱逐
        c.put(3, 3);                // 驱逐 freq 最低的 2
        assert(c.get(2) == -1);
        assert(c.get(1) == 11);
        assert(c.get(3) == 3);
    }

    // Test 4: minFreq 回归 —— 高频 key 存在时, 新 key 插入后 minFreq 必须归 1
    {
        LFUCache c(2);
        c.put(1, 1);
        c.get(1); c.get(1); c.get(1);   // freq(1)=4
        c.put(2, 2);                    // freq(2)=1, minFreq 必须 = 1
        c.put(3, 3);                    // 驱逐的必须是 2, 不是 1
        assert(c.get(2) == -1);
        assert(c.get(1) == 1);
        assert(c.get(3) == 3);
    }

    // Test 5: capacity = 0
    {
        LFUCache c(0);
        c.put(1, 1);
        assert(c.get(1) == -1);
    }

    // Test 6: 单容量反复驱逐
    {
        LFUCache c(1);
        c.put(1, 1);
        c.get(1); c.get(1);         // freq(1)=3
        c.put(2, 2);                // 满: 唯一条目 1 被驱逐(哪怕它频次高)
        assert(c.get(1) == -1);
        assert(c.get(2) == 2);
    }

    cout << "All tests passed.\n";
    return 0;
}