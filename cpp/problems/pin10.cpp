#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <queue>
using namespace std;

// ============================================================
// 一群朋友间的费用结算。给定若干笔交易 (from, to, amount):
//   from 付给 to amount 元。
// 计算每个人的净余额后,求【最少多少笔转账】能结清所有债务。
// 返回最少转账笔数。
//
// 例: transactions = [("A","B",10), ("B","C",5)]
//   净额: A=-10, B=+5, C=+5  (A欠10, B和C各被欠5... 等下重算)
//   A付B10 -> A:-10 B:+10; B付C5 -> B:+5 C:+5
//   净: A=-10, B=+5, C=+5
//   结算: A付B5, A付C5 = 2笔  (或其他, 最少2笔)
// ============================================================

class ExpenseSettler {
public:
    // 返回结清所有债务的最少转账笔数
    int minTransactions(vector<tuple<string,string,int>>& transactions) {
        // 你写:
        // 1) 算每个人净余额 (收到的 - 付出的)
        // 2) 取出所有非零余额 (正=债权, 负=债务)
        // 3) 回溯/贪心: 最少笔数结算
        unordered_map<string, int> own;
        for (auto &[x, y, m] : transactions) {
            own[x] -= m;
            own[y] += m;
        }
        vector<int> own1;
        int n =0;
        for (auto &[x, m] :own) {
            if (m !=0) {
                n++;
                own1.push_back(m);
            }
        }
        int t =1<<own1.size();
        vector<int> dp(t, 0);
        vector<int> sum(t, 0);
        
        for (int i=0;i<t;i++) {
            for (int j=0;j<own1.size();j++) {
                if (((i >> j) & 1) ==1) {
                    sum[i] += own1[j];
                }
            }
        }
        for (int i=0;i<t;i++) {
            int sub =i;
            if (sum[i] !=0) {
                continue;
            }
            while (sub !=0) {
                if (sum[sub] ==0) {
                    dp[i] =max(dp[i ^ sub] +1, dp[i]);
                }
                sub =i & (sub-1);
            }
        }
        return n - dp[t-1];
    }
};

// ============ TEST ============
void run(const string& name, vector<tuple<string,string,int>> txns, int expected) {
    ExpenseSettler s;
    int got = s.minTransactions(txns);
    cout << name << ": got=" << got << " expected=" << expected
         << (got == expected ? "  PASS" : "  *** FAIL ***") << "\n";
}

int main() {
    run("simple",   {{"A","B",10},{"B","C",5}}, 2);
    // A=-10,B=+5,C=+5 -> A付B5,A付C5 = 2
    run("already",  {{"A","B",5},{"B","A",5}}, 0);   // 互相抵消 -> 0
    run("chain",    {{"A","B",10},{"B","C",10},{"C","A",10}}, 0); // 环, 净额全0 -> 0
    run("three",    {{"A","B",5},{"A","C",5},{"B","D",5}}, 3);
    // A=-10, B=0, C=+5, D=+5 -> A付C5,A付D5 = 2  (B净0不参与)
    run("single",   {{"A","B",100}}, 1);  // A付B100 = 1
    run("empty",    {}, 0);
    return 0;
}