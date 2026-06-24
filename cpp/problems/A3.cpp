#include <vector>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <random>
using namespace std;

int maxDistinctInWindow(const std::vector<int>& log, int k) {
    if (k<=0 || k>log.size()) return 0;
    unordered_map<int, int> fre;
    for (int i=0;i <k;i++) {
        fre[log[i]]++;
    }

    size_t ret =fre.size();
    for (int i=k;i<log.size();i++) {
        fre[log[i]]++;
        fre[log[i-k]]--;
        if (fre[log[i-k]]==0) {
            fre.erase(log[i-k]);
        }
        ret =max(ret, fre.size());
    }
    return ret;
}


int bruteForce(const vector<int>& log, int k) {
    int n = log.size();
    if (k <= 0 || k > n) return 0;
    int best = 0;
    for (int i = 0; i + k <= n; ++i) {
        unordered_set<int> s;
        for (int j = i; j < i + k; ++j) s.insert(log[j]);
        best = max(best, (int)s.size());
    }
    return best;
}

void runCase(const string& name, const vector<int>& log, int k) {
    int got  = maxDistinctInWindow(log, k);
    int want = bruteForce(log, k);
    cout << "[" << (got == want ? "PASS" : "FAIL") << "] " << name
         << "  got=" << got << " want=" << want << " (k=" << k << ")\n";
}

int main() {
    runCase("example",            {1,2,1,3,4}, 3);
    runCase("empty log",          {}, 3);
    runCase("k=0",                {1,2,3}, 0);
    runCase("k negative",         {1,2,3}, -2);
    runCase("k > n",              {1,2}, 5);
    runCase("k == n",             {1,2,2,3}, 4);
    runCase("k == 1",             {5,5,5}, 1);
    runCase("all same",           {7,7,7,7}, 2);
    runCase("all distinct",       {1,2,3,4,5}, 3);
    runCase("dup at window edge",  {1,2,1,2,1}, 3);
    runCase("single element",     {9}, 1);

    std::mt19937 rng(2024);
    std::uniform_int_distribution<int> nDist(0, 30);
    std::uniform_int_distribution<int> idDist(1, 6);
    std::uniform_int_distribution<int> kDist(-2, 32);

    int trials = 5000, passed = 0;
    for (int t = 0; t < trials; ++t) {
        int n = nDist(rng);
        vector<int> log(n);
        for (int i = 0; i < n; ++i) log[i] = idDist(rng);
        int k = kDist(rng);
        if (maxDistinctInWindow(log, k) == bruteForce(log, k)) ++passed;
        else {
            cout << "[FAIL] trial " << t << " k=" << k << " log=";
            for (int x : log) cout << x << " ";
            cout << "\n";
        }
    }
    cout << "random oracle: " << passed << "/" << trials << " passed\n";

    return 0;
}