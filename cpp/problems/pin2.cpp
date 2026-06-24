#include <iostream>
#include <vector>
#include <queue>
#include <cassert>
using namespace std;

// ============ YOUR SOLUTION ============
// Return true if, starting from `start`, you can reach any index with value 0.
// At index i you may jump exactly arr[i] steps left or right, wrapping around (mod n).
bool canReachZero(const vector<int>& arr, int start) {
    // TODO: 你来填
    queue<int> qu;
    int n =arr.size();
    if (start <0 || start >=n) {
        return false;
    }
    if (arr[start] ==0) {
        return true;
    }
    qu.push(start);
    vector<bool> flag(n, false);
    flag[start] =true;
    while (!qu.empty()) {
        int x =qu.front();
        qu.pop();
        int r =(x + arr[x])%n;
        if (arr[r] ==0) {
            return true;
        }
        if (!flag[r]) {
            flag[r] =true;
            qu.push(r);
        }
        int l =(x - arr[x] + n)%n;
        if (arr[l] ==0) {
            return true;
        }
        if (!flag[l]) {
            flag[l] =true;
            qu.push(l);
        }
    }
    return false;
}

// ============ TEST HARNESS ============
void run(const string& name, const vector<int>& arr, int start, bool expected) {
    bool got = canReachZero(arr, start);
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main() {
    // 1) 基本: start 走到 index2 的 0
    run("basic_reach",        {1,2,0,4,3}, 0, true);

    // 2) start 本身就是 0
    run("start_is_zero",      {1,2,0,4,3}, 2, true);

    // 3) 无法到达任何 0 (构造一个困死的)
    run("unreachable",        {2,2,2,2},   0, false);   // 全是2,n=4,只在偶数 index 间跳,无0其实->false

    // 4) 没有任何 0
    run("no_zero",            {1,1,1,1},   0, false);

    // 5) 单元素是 0
    run("single_zero",        {0},         0, true);

    // 6) 单元素非0 (自己跳自己,绕回还是自己,死循环风险 -> 必须靠 visited)
    run("single_nonzero",     {3},         0, false);

    // 7) 绕回到达: start=0, arr[0]=4, n=5 -> (0+4)%5=4, arr[4]=... 设计成能绕回踩到0
    run("wrap_reach",         {4,1,1,1,0}, 0, true);     // 0 ->(0+4)%5=4 = 0  true

    // 8) 需要左跳: start 在右边,靠左跳到 0
    run("left_jump",          {0,1,2,1,2}, 4, true);     // 4 ->(4-2)%5=2 ->(2-2)=0 = 0 true

    // 9) 空数组 (边界)
    run("empty",              {},          0, false);

    // 10) start 越界 (边界,看你要不要 guard)
    run("start_oob",          {1,0,1},     5, false);

    return 0;
}