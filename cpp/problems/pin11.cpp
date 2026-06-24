#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
using namespace std;

// ============================================================
// 两个人的日历, 返回两人都空闲的时段。
// 一天 [0.0, 24.0], 时间是浮点。日历不一定有序。
//
// 思路(你的): A+B 忙碌时段合并 -> 排序 -> merge intervals(忙的并集)
//            -> 从 [0,24] 减去并集 = 都空闲的时段
// ============================================================

class CalendarMerger {
public:
    // busyA, busyB: 各自的忙碌时段 [start, end]
    // 返回: 两人都空闲的时段 [start, end], 按时间升序
    vector<pair<double,double>> freeSlots(
        vector<pair<double,double>>& busyA,
        vector<pair<double,double>>& busyB) {
        // 你写:
        // 1) 合并 busyA + busyB 到一个 vector
        // 2) 排序
        // 3) merge overlapping intervals -> 忙的并集
        // 4) 扫描并集, 取相邻忙时段之间的空隙(以及 0 到第一个、最后一个到 24)
        vector<pair<double, double>> all;
        for (auto &a : busyA) {
            all.push_back(a);
        }
        for (auto &b : busyB) {
            all.push_back(b);
        }
        vector<pair<double, double>> ret;

        if (all.size()==0) {
            ret.push_back({0, 24});
            return ret;
        }
        sort(all.begin(), all.end());
        vector<pair<double, double>> merge;
        merge.push_back(all[0]);
        for (auto & [s, e] : all) {
            if (s <= merge.back().second) {
                merge.back().second =max(merge.back().second, e);
            }
            else {
                merge.push_back({s, e});
            }
        }        
        double s =0;
        
        if (s < merge.front().first) {
            ret.push_back({0, merge.front().first});
        }
        for (int i=0;i<merge.size()-1;i++) {
            ret.push_back({merge[i].second, merge[i+1].first});
        }
        if (merge.back().second <24) {
            ret.push_back({merge.back().second, 24});
        }
        return ret;
    }
};

// ============ TEST ============
void run(const string& name,
         vector<pair<double,double>> a,
         vector<pair<double,double>> b,
         vector<pair<double,double>> expected) {
    CalendarMerger c;
    auto got = c.freeSlots(a, b);
    bool pass = (got == expected);
    cout << name << ": " << (pass ? "PASS" : "*** FAIL ***") << "\n";
    cout << "  got:      ";
    for (auto& [s,e] : got) cout << "[" << s << "," << e << "] ";
    cout << "\n  expected: ";
    for (auto& [s,e] : expected) cout << "[" << s << "," << e << "] ";
    cout << "\n";
}

int main() {
    // 例子: A忙[9,10.5][13,14], B忙[10,11][15,16]
    // 并集: [9,11][13,14][15,16]
    // 空闲: [0,9][11,13][14,15][16,24]
    run("basic",
        {{9,10.5},{13,14}}, {{10,11},{15,16}},
        {{0,9},{11,13},{14,15},{16,24}});

    // 无序输入 (测排序)
    run("unsorted",
        {{13,14},{9,10.5}}, {{15,16},{10,11}},
        {{0,9},{11,13},{14,15},{16,24}});

    // 两人都不忙 -> 全天空闲
    run("all_free",
        {}, {},
        {{0,24}});

    // 一人忙满全天 -> 无空闲
    run("full_busy",
        {{0,24}}, {},
        {});

    // 重叠忙碌 (A,B 时段重叠, merge 后是一段)
    run("overlap",
        {{9,12}}, {{10,14}},
        {{0,9},{14,24}});

    // 相邻接壤 [9,10] 和 [10,11] -> 合并成 [9,11] (用 <= 判接壤)
    run("touching",
        {{9,10}}, {{10,11}},
        {{0,9},{11,24}});

    // 忙到边界 [0, 8]
    run("from_zero",
        {{0,8}}, {},
        {{8,24}});

    // 忙到 24
    run("to_end",
        {{20,24}}, {},
        {{0,20}});

    return 0;
}