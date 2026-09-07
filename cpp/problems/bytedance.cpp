#include <bits/stdc++.h>
#include <list>
#include <assert.h>
#include <queue>
#include <unordered_map>
#include <functional>
using namespace std;

class MemoryStore {
    int _capacity;
    struct entry {
        string key;
        long long ttl;
        string value;
        list<string>::iterator p;
    };

    unordered_map<string, entry> _cache;
    list<string> _link;

    struct Cmp {
        bool operator()(const struct entry & A, const struct entry &B) const {
            return A.ttl > B.ttl;
        }
    };

    priority_queue<entry, vector<entry>, Cmp> _qu;

    void removeTTL(long long timestamp) {
        while(!_qu.empty() && _qu.top().ttl <= timestamp) {
            auto e1 = _qu.top();
            string key =e1.key;
            _qu.pop();
            if (_cache.count(key)> 0 && e1.ttl == _cache[key].ttl && e1.value == _cache[key].value) {
                _link.erase(_cache[key].p);
                _cache.erase(key);
            }
        } 
    }

public:
    explicit MemoryStore(int capacity) {
        _capacity =capacity;
        assert(_capacity >0);
    }

    // 写入/覆盖。存活至 timestamp + ttl(即 expire = timestamp + ttl,
    // t < expire 时有效,t >= expire 视为过期)

    void put(long long timestamp, const string& key, const string& value, long long ttl) {
        assert(ttl >0);
        removeTTL(timestamp);
        if (_cache.count(key) >0) {
            _link.erase(_cache[key].p);  
            _cache.erase(key);         
        }
        if (_link.size() == _capacity) {
            string str =_link.back();
            _cache.erase(str);
            _link.pop_back();
        }
        entry en;
        en.key =key;
        en.value =value;
        en.ttl = timestamp + ttl;

        _link.push_front(key);
        en.p =_link.begin();
        _qu.push(en);
        
        _cache[key] =en;
    }

    // 未过期返回 value,否则返回 ""
    string get(long long timestamp, const string& key) {
        removeTTL(timestamp);
        if (_cache.count(key) ==0) {
            return "";
        }
        string val =_cache[key].value;
        _link.erase(_cache[key].p);
        _link.push_front(key);
        _cache[key].p =_link.begin();
        return val;
    }

private:
    // 你来填:hashmap / 双链表 / 小顶堆
};

// ---------------- Testing ----------------
int main() {
    // Test 1: 基本 put/get
    {
        MemoryStore ms(2);
        ms.put(0, "a", "1", 10);
        assert(ms.get(1, "a") == "1");
        assert(ms.get(1, "b") == "");
    }

    // Test 2: TTL 过期(边界:t == expire 算过期)
    {
        MemoryStore ms(2);
        ms.put(0, "a", "1", 5);          // expire = 5
        assert(ms.get(4, "a") == "1");
        assert(ms.get(5, "a") == "");    // 恰好到期
        assert(ms.get(6, "a") == "");
    }

    // Test 3: overwrite 更新 TTL —— stale heap entry 不得误删
    {
        MemoryStore ms(2);
        ms.put(0, "k", "v1", 5);         // 旧 expire = 5
        ms.put(1, "k", "v2", 100);       // 新 expire = 101
        assert(ms.get(6, "k") == "v2");  // 弹到 (5,k) 必须判 stale 丢弃
        assert(ms.get(100, "k") == "v2");
        assert(ms.get(101, "k") == "");
    }

    // Test 4: LRU 驱逐(无过期时,驱逐最久未访问)
    {
        MemoryStore ms(2);
        ms.put(0, "a", "1", 1000);
        ms.put(1, "b", "2", 1000);
        ms.get(2, "a");                  // a 变新,b 是 LRU
        ms.put(3, "c", "3", 1000);       // 满,驱逐 b
        assert(ms.get(4, "a") == "1");
        assert(ms.get(4, "b") == "");
        assert(ms.get(4, "c") == "3");
    }

    // Test 5: "看起来满其实没满" —— 过期条目不占 capacity
    {
        MemoryStore ms(2);
        ms.put(0, "a", "1", 3);          // expire = 3
        ms.put(1, "b", "2", 1000);
        ms.put(5, "c", "3", 1000);       // a 已过期:清 a,不驱逐 b
        assert(ms.get(6, "b") == "2");
        assert(ms.get(6, "c") == "3");
    }

    // Test 6: 被 LRU 驱逐的 key,其堆条目变 stale,后续不得误伤同名新 key
    {
        MemoryStore ms(1);
        ms.put(0, "a", "1", 5);          // 堆里 (5, a)
        ms.put(1, "b", "2", 1000);       // 驱逐 a,(5,a) 变 stale
        ms.put(2, "a", "3", 1000);       // 驱逐 b,a 重新插入,expire = 1002
        assert(ms.get(6, "a") == "3");   // 弹 (5,a) 时 expire 不匹配,丢弃
    }

    // Test 7: get 也算访问(影响 LRU 序)—— Test 4 已覆盖,这里测 put 覆盖也刷新
    {
        MemoryStore ms(2);
        ms.put(0, "a", "1", 1000);
        ms.put(1, "b", "2", 1000);
        ms.put(2, "a", "9", 1000);       // 覆盖 a,a 变新
        ms.put(3, "c", "3", 1000);       // 驱逐 b
        assert(ms.get(4, "a") == "9");
        assert(ms.get(4, "b") == "");
    }

    cout << "All tests passed.\n";
    return 0;
}