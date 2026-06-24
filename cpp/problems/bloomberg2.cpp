#include <map>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <iostream>

using namespace std;

class TextInfoManage {
private:
    unordered_map<long long, map<string, string>> _cache; 
public:
    string get(long long statute_id, const string & time) {
        if (_cache.find(statute_id) == _cache.end()) {
            return "";
        }
        auto & data = _cache[statute_id];
        auto iter = data.upper_bound(time);
        if (iter != data.begin()) {
            --iter;
            return iter->second;
        }
        return "";
    }

    void set(long long statute_id, const string & time, const string &text) {
        _cache[statute_id][time] =text;
    }
};

int main() {
    TextInfoManage ma;
    
    // 测试设置
    ma.set(1, "2024-01-01", "version 1");
    ma.set(1, "2024-01-03", "version 2");
    ma.set(1, "2024-01-05", "version 3");
    
    // 测试获取
    cout << ma.get(1, "2024-01-02") << endl;  // 期望: version 1 (<= 1月2日的最新版本)
    cout << ma.get(1, "2024-01-03") << endl;  // 期望: version 2 (等于)
    cout << ma.get(1, "2024-01-04") << endl;  // 期望: version 2 (<= 1月4日的最新版本)
    cout << ma.get(1, "2024-01-06") << endl;  // 期望: version 3 (最后一个)
    cout << ma.get(1, "2023-12-31") << endl;  // 期望: "" (没有更早的)
    cout << ma.get(2, "2024-01-01") << endl;  // 期望: "" (statute_id 不存在)
    
    return 0;
}