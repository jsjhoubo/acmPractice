#include <bits/stdc++.h>
#include <functional>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS.
//  Return true if `word` can be traced on the board via
//  4-directional adjacency, no cell reused within one path.
// ============================================================
bool exist(vector<vector<char>>& board, const string& word) {
    // TODO: your code here
    if (word.size() ==0) {
        return false;
    }
    size_t n =board.size();
    if (n ==0) {
        return false;
    }
    size_t m =board[0].size();
    if (m==0) {
        return false;
    }

    int dir[4][2]={{-1,0}, {1, 0}, {0, -1}, {0,1}};
    auto valid=[&](int r, int c) {
        return r>=0 && r< n && c>=0 && c<m;
    };
    vector<vector<bool>> visited(n, vector<bool>(m, false));
    
    function<bool(int, int, int)> dfs =[&](int  r, int c, int d) ->bool {
        if (d ==word.size()) {
            return true;
        }
        if (!(valid(r,c) && !visited[r][c] && d<word.size() && board[r][c]==word[d])) {
            return false;
        }
        visited[r][c] =true;
        for (int i=0;i<4;i++) {
            int r1 =r +dir[i][0];
            int c1 =c +dir[i][1];
            bool flag =dfs(r1, c1, d+1);
            if (flag) {
                return true;
            }
        }
        visited[r][c] =false;
        return false;
    };

    for (int i=0;i<n;i++) {
        for (int j=0;j<m;j++) {
            bool flag =dfs(i, j, 0);
            if (flag) {
                return true;
            }
        }
    }

    return false;
}

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed=0, failed=0;
static void chk(const string& name, vector<vector<char>> board, const string& word, bool expected) {
    bool got = exist(board, word);   // pass by value so each test gets a fresh board
    if (got==expected) { passed++; printf("  [PASS] %s: exist(\"%s\") = %s\n", name.c_str(), word.c_str(), got?"true":"false"); }
    else { failed++; printf("  [FAIL] %s: exist(\"%s\") = %s, expected %s\n", name.c_str(), word.c_str(), got?"true":"false", expected?"true":"false"); }
}

int main() {
    vector<vector<char>> b = {
        {'A','B','C','E'},
        {'S','F','C','S'},
        {'A','D','E','E'}
    };
    chk("spec_true1", b, "ABCCED", true);
    chk("spec_true2", b, "SEE", true);
    chk("spec_false_reuse", b, "ABCB", false);   // B only once, cannot reuse

    // single cell
    chk("single_match", {{'A'}}, "A", true);
    chk("single_nomatch", {{'A'}}, "B", false);

    // word longer than board cells -> impossible
    chk("too_long", {{'A','B'}}, "ABABAB", false);

    // path requires backtracking: dead-end then alt route
    // board:
    //  A A A A
    //  A A A A
    //  A A A A
    // word "AAAAAAAAAAAA" (12 A's, exactly all cells) -> true (Hamiltonian-ish snake)
    {
        vector<vector<char>> g(3, vector<char>(4,'A'));
        chk("all_A_fill", g, "AAAAAAAAAAAA", true);      // 12 A's == 12 cells
        chk("all_A_toolong", g, "AAAAAAAAAAAAA", false); // 13 A's > 12 cells
    }

    // must NOT reuse: 2x2 all A, word "AAAAA" (5) impossible (only 4 cells)
    chk("reuse_blocked", {{'A','A'},{'A','A'}}, "AAAAA", false);

    // straight line
    chk("straight", {{'A','B','C','D'}}, "ABCD", true);
    chk("straight_rev", {{'A','B','C','D'}}, "DCBA", true);

    // needs turn (not straight)
    {
        vector<vector<char>> g = {{'A','B'},{'D','C'}};
        chk("turn_path", g, "ABCD", true);   // A(0,0)->B(0,1)->C(1,1)->D(1,0)
    }

    // first-letter appears multiple times, only one entry works
    {
        vector<vector<char>> g = {
            {'C','A','A'},
            {'A','A','A'},
            {'B','C','D'}
        };
        chk("multi_entry", g, "AAB", true);  // some A-A-B path exists
    }

    // char present but path broken (not adjacent)
    chk("broken_path", {{'A','B'},{'C','D'}}, "AD", false);  // A and D not adjacent

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed==0 ? 0 : 1;
}