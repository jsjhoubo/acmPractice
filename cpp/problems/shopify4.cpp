#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
using namespace std;

class KVStore {
    // TODO: 你的数据结构。提示：想想"基础层 + 一摞事务层"怎么表示
    vector<unordered_map<string, string>> _cache;
    unordered_map<string, string> _data;
public:
    void set(const string& key, const string& value) {
        // TODO_
        if (_cache.size()==0) {
            _data[key] =value;
            return;
        }
        _cache.back()[key] =value;
    }
    // 不存在返回 ""
    string get(const string& key) {
        // TODO
        if (_cache.size()>0) {
            if (_cache.back().count(key) >0) {
                return _cache.back()[key];
            }
            return "";
        }
        if (_data.count(key) >0) {
            return _data[key];
        }
        return "";
    }
    void del(const string& key) {
        // TODO
        if (_cache.size() ==0) {
            _data.erase(key);
            return;
        }
        _cache.back().erase(key);
    }
    void begin() {
        if (_cache.size() >0) {
            _cache.push_back(_cache.back());
            return;
        }
        _cache.push_back(unordered_map<string, string>());
    }
    bool commit() {   // 无活动事务返回 false
        // TODO
        if (_cache.size()>0) {
            if (_cache.size()-1>0) {
                size_t n= _cache.size();
                for (auto & [k, v] : _cache.back()) {
                    _cache[n-2][k] =v;
                }
            }
            else {
                for (auto & [k, v] : _cache.back()) {
                    _data[k] =v;
                }
            }
            _cache.pop_back();
            return true;
        }
        return false;
    }
    bool rollback() { // 无活动事务返回 false
        // TODO
        if (_cache.size()>0) {
            _cache.pop_back();
            return true;
        }
        return false;
    }
};

void check(const string& name, bool ok) {
    cout << (ok ? "[PASS] " : "[FAIL] ") << name << "\n";
}

int main() {
    // 1. 基础读写
    { KVStore s; s.set("a","1");
      check("get a = 1", s.get("a")=="1");
      check("get missing = empty", s.get("x")==""); }

    // 2. 删除
    { KVStore s; s.set("a","1"); s.del("a");
      check("after del, a empty", s.get("a")==""); }

    // 3. 事务内可见自己的改动
    { KVStore s; s.begin(); s.set("a","1");
      check("see own write in txn", s.get("a")=="1"); }

    // 4. rollback 丢弃改动
    { KVStore s; s.set("a","1");
      s.begin(); s.set("a","2"); s.rollback();
      check("rollback restores a=1", s.get("a")=="1"); }

    // 5. commit 生效
    { KVStore s; s.set("a","1");
      s.begin(); s.set("a","2"); s.commit();
      check("commit keeps a=2", s.get("a")=="2"); }

    // 6. 嵌套：内层 rollback，外层保留
    { KVStore s;
      s.begin(); s.set("a","1");
      s.begin(); s.set("a","2"); s.rollback();
      check("nested: inner rollback -> a=1", s.get("a")=="1");
      s.commit();
      check("outer commit -> a=1 persists", s.get("a")=="1"); }

    // 7. 嵌套：内层 commit 合并到外层，外层 rollback 全丢
    { KVStore s; s.set("a","0");
      s.begin(); s.set("a","1");
      s.begin(); s.set("a","2"); s.commit();   // 内层 commit，a=2 并入外层
      check("after inner commit a=2", s.get("a")=="2");
      s.rollback();                             // 外层 rollback，全丢回 0
      check("outer rollback -> a=0", s.get("a")=="0"); }

    // 8. 无事务时 commit/rollback 报错
    { KVStore s;
      check("commit no txn = false", s.commit()==false);
      check("rollback no txn = false", s.rollback()==false); }

    // 9. del 在事务里 + rollback
    { KVStore s; s.set("a","1");
      s.begin(); s.del("a");
      check("del visible in txn", s.get("a")=="");
      s.rollback();
      check("rollback restores deleted a=1", s.get("a")=="1"); }
}