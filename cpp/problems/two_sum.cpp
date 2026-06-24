#include <iostream>
#include <vector>
#include <unordered_map>
#include <algorithm>
using namespace std;

vector<pair<int,int>> twoSum(vector<int>& nums, int target) {
    // your code here
    sort(nums.begin(), nums.end());
    int l = 0;
    int r = nums.size()-1;
    vector<pair<int, int>> ret;
    while (l < r) {
        int sum =nums[l] + nums[r];
        if ( sum ==target) {
            if (l==0 ||(l>0 && nums[l] > nums[l-1])) {
                ret.push_back({nums[l], nums[r]});
            }
            l++,r--;
        }
        else if (sum < target) {
            l++;
        }
        else {
            r --;
        }
    }
    return ret;
}

int main() {
    vector<int> nums1 = {1, 3, 2, 4, 3, 2, 1};
    auto res1 = twoSum(nums1, 4);
    for (auto& p : res1)
        cout << "(" << p.first << "," << p.second << ") ";
    cout << endl;  // expected: (1,3) (2,2)

    vector<int> nums2 = {1, 1, 1, 1};
    auto res2 = twoSum(nums2, 2);
    for (auto& p : res2)
        cout << "(" << p.first << "," << p.second << ") ";
    cout << endl;  // expected: (1,1)

    vector<int> nums3 = {1, 2, 3};
    auto res3 = twoSum(nums3, 10);
    for (auto& p : res3)
        cout << "(" << p.first << "," << p.second << ") ";
    cout << endl;  // expected: (empty)

    return 0;
}