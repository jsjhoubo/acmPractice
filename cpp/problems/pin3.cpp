#include <iostream>
#include <vector>
#include <string>
using namespace std;

// ============ YOUR SOLUTION (recursive DFS) ============
// Helper: DFS from index i, marking visited, return true if reach any 0.
bool dfs(const vector<int>& arr, int i, vector<bool>& visited) {
    visited[i] =true;
    if (arr[i] ==0) {
        return true;
    }
    for (auto jump :{arr[i], -arr[i]}) {
        int x = (i + jump + arr.size())%arr.size();
        if (!visited[x]) {
            bool ret =dfs(arr, x, visited);
            if (ret) {
                return ret;
            }
        }
    }
    return false;
}

// Return true if, starting from `start`, you can reach any index with value 0.
// At index i you may jump exactly arr[i] steps left or right, wrapping around (mod n).
bool canReachZero(const vector<int>& arr, int start) {
    int n = arr.size();
    if (n ==0) {
        return false;
    }
    if (start < 0 || start >= n) return false;
    vector<bool> visited(n, false);
    return dfs(arr, start, visited);
}

// ============ TEST HARNESS ============
void run(const string& name, const vector<int>& arr, int start, bool expected) {
    bool got = canReachZero(arr, start);
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main() {
    run("basic_reach",        {1,2,0,4,3}, 0, true);
    run("start_is_zero",      {1,2,0,4,3}, 2, true);
    run("unreachable",        {2,2,2,2},   0, false);
    run("no_zero",            {1,1,1,1},   0, false);
    run("single_zero",        {0},         0, true);
    run("single_nonzero",     {3},         0, false);
    run("wrap_reach",         {4,1,1,1,0}, 0, true);
    run("left_jump",          {0,1,2,1,2}, 4, true);
    run("empty",              {},          0, false);
    run("start_oob",          {1,0,1},     5, false);
    return 0;
}