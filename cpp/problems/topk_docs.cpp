#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <tuple>
#include <string>
#include <queue>
#include <functional>
#include <algorithm>
#include <ranges>

using namespace std;

map<string, vector<string>> topKDocs(
    vector<tuple<string,string,float>>& data, int k) {
    
    unordered_map<string, vector<pair<string, float>>> dict;
    for (auto & [q, d, score] : data) {
        dict[q].push_back({d, score});
    }
    
    auto partition = [](int l, int r, vector<pair<string, float>>& arr) -> int {
        int pivot = l;
        while (l < r) {
            while (l < r && arr[pivot].second > arr[r].second) {
                r--;
            }
            std::swap(arr[r], arr[pivot]);
            pivot = r;
            while (l < r && arr[pivot].second < arr[l].second) {
                l++;
            }
            std::swap(arr[l], arr[pivot]);
            pivot = l;
        }
        return pivot;
    };
    
    // ✅ 修正：参数顺序统一为 (k, l, r, arr)
    std::function<void(int, int, int, vector<pair<string, float>>&)> topKFunc = 
        [&](int k, int l, int r, vector<pair<string, float>>& arr) {
            if (l >= r || k <= 0) return;  // 添加边界条件
            int p = partition(l, r, arr);
            int left_len = p - l + 1;
            
            if (left_len == k) {
                return;
            }
            else if (left_len < k) {
                topKFunc(k - left_len, p + 1, r, arr);
            }
            else {
                topKFunc(k, l, p - 1, arr);
            }
        };
    map<string, vector<string>> ret;
    for (auto & [q, v] : dict) {
        if (v.size() <= k) {
            continue;  // 不需要找 topK，直接全部保留
        }
        topKFunc(k, 0, v.size() - 1, v);
    }
    
    for (auto & [q, v] : dict) {
        v.resize(min(k, (int)v.size()));
        std::sort(v.begin(), v.end(), 
                  [](auto& a, auto& b) { return a.second < b.second; });
    }
    
    
    for (auto & [q, v] : dict) {
        // ✅ 修正拼写：reserve → reverse
        for (auto &[d, score] : v | std::views::reverse) {
            ret[q].push_back(d);
        }
    }
    return ret;
}

int main() {
    // test 1: normal case
    vector<tuple<string,string,float>> data1 = {
        {"q1", "d1", 0.9},
        {"q1", "d2", 0.3},
        {"q1", "d3", 0.7},
        {"q2", "d1", 0.5},
        {"q2", "d4", 0.8}
    };
    auto res1 = topKDocs(data1, 2);
    for (auto& [q, docs] : res1) {
        cout << q << ": ";
        for (auto& d : docs) cout << d << " ";
        cout << endl;
    }
    // expected: q1: d1 d3, q2: d4 d1

    cout << "---" << endl;

    // test 2: k larger than available docs
    vector<tuple<string,string,float>> data2 = {
        {"q1", "d1", 0.5},
        {"q1", "d2", 0.9}
    };
    auto res2 = topKDocs(data2, 5);
    for (auto& [q, docs] : res2) {
        cout << q << ": ";
        for (auto& d : docs) cout << d << " ";
        cout << endl;
    }
    // expected: q1: d2 d1

    cout << "---" << endl;

    // test 3: single doc per query
    vector<tuple<string,string,float>> data3 = {
        {"q1", "d1", 0.7},
        {"q2", "d2", 0.3}
    };
    auto res3 = topKDocs(data3, 2);
    for (auto& [q, docs] : res3) {
        cout << q << ": ";
        for (auto& d : docs) cout << d << " ";
        cout << endl;
    }
    // expected: q1: d1, q2: d2

    return 0;
}