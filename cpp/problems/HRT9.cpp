#include <cassert>
#include <iostream>
#include <vector>

using namespace std;
// state
// nums 输入数组
// n 数组大小
// 算法
// 尽量让 正整数x，占据x-1的位置
// 然后数组扫一遍如果 nums[i] != i+1 ,返回i+1 就行

// 对于当前位置i
//   看一下这个数字的范围，如果在 1和n 之间那么就将 nums[i] 换到 位置nums【i】-1 不在1和n之间break
//   如果当前nums[i] 等于i+1 break了
//   如果nums[i]和nums[nums[i]-1] 一样也要break 不然死循环

// 1 2 0
// i=0 nothing 
// i=1 nothing
// i=2 nothing
// scan return 3


// 3 4 -1, 1
// i =0 
//    -1 4 3 1
// i =1
//    -1 1 3 4
//    1 -1 3 4
// i =2 
// i =3
// 扫一遍  nums[1]!=2 return 2


int firstMissingPositive(vector<int> nums) {
    // TODO
    int n =nums.size();
    for (int i=0;i<n;i++) {
        while (true) {
            if (!(nums[i] >=1 && nums[i] <=n)) {
                break;
            }
            if (nums[i] == nums[nums[i]-1]) {
                break;
            }
            if (nums[i] ==i+1) {
                break;
            }
            std::swap(nums[i], nums[nums[i]-1]);
        }
    }
    for (int i=0;i<n;i++) {
        if (nums[i] !=i+1) {
            return i+1;
        }
    }
    return n+1;
}

int main() {
    assert(firstMissingPositive({1,2,0}) == 3);
    assert(firstMissingPositive({3,4,-1,1}) == 2);
    assert(firstMissingPositive({7,8,9,11,12}) == 1);

    assert(firstMissingPositive({1}) == 2);
    assert(firstMissingPositive({2}) == 1);
    assert(firstMissingPositive({}) == 1);

    assert(firstMissingPositive({1,1}) == 2);
    assert(firstMissingPositive({2,2}) == 1);
    assert(firstMissingPositive({1,2,3,4}) == 5);

    assert(firstMissingPositive({2,1}) == 3);
    assert(firstMissingPositive({4,3,2,1}) == 5);

    cout << "All tests passed!\n";
}