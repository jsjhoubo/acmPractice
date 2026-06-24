#include <iostream>
#include <vector>
#include <functional>
#include <string>
using namespace std;

int countBadRegions(vector<vector<int>>& grid) {
    int dir[4][2] ={{-1, 0}, {1, 0}, {0 ,1}, {0,-1}};
    int cnt =0;
    int n =grid.size();
    if (n ==0) {
        return 0;
    }
    int m = grid[0].size();
    if (m ==0) return 0;
    auto valid =[&](int r, int c) {
        return r >=0 && r<n && c>=0 && c<m;
    };
    std::function<void(int,int)> dfs =[&](int r, int c) {
        for (int i=0;i<4;i++) {
            int r1 =r + dir[i][0];
            int c1 =c + dir[i][1];
            if (valid(r1, c1) && grid[r1][c1] ==1 ) {
                grid[r1][c1] =2;
                dfs(r1, c1);
            }
        }
    };
    for (int i=0;i<n;i++) {
        for (int j=0;j<m;j++) {
            if (grid[i][j] ==1) {
                dfs(i, j);
                cnt ++;
            }
        }
    }
    return cnt;
}




// ... 你的 countBadRegions ...

void run(const string& name, vector<vector<int>> grid, int expected) {
    int got = countBadRegions(grid);   // 传值,因为函数会改 grid
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main() {
    // 1) 题目例子 (4x5 长方形 — 专门测你的 valid bug!)
    run("example_2regions", {
        {1,1,0,0,0},
        {1,1,0,0,1},
        {0,0,0,1,1},
        {0,0,0,0,0}
    }, 2);

    // 2) 全 0
    run("all_zero", {{0,0},{0,0}}, 0);

    // 3) 全 1 (连成一片)
    run("all_one", {{1,1},{1,1}}, 1);

    // 4) 棋盘状 (对角不算相连,每个 1 独立)
    run("checkerboard", {
        {1,0,1},
        {0,1,0},
        {1,0,1}
    }, 5);

    // 5) 单格 1
    run("single_one", {{1}}, 1);

    // 6) 单格 0
    run("single_zero", {{0}}, 0);

    // 7) 长方形 — 横条 (1行5列,再次测 n/m 混淆)
    run("wide_row", {{1,0,1,1,0}}, 2);

    // 8) 长方形 — 竖条 (5行1列)
    run("tall_col", {{1},{1},{0},{1},{0}}, 2);

    // 9) 空网格
    run("empty", {}, 0);

    // 10) L形连通
    run("L_shape", {
        {1,0,0},
        {1,0,0},
        {1,1,1}
    }, 1);

    return 0;
}