#include <bits/stdc++.h>
#include <queue>
#include <algorithm>
using namespace std;

// ==== YOU IMPLEMENT THIS ====
vector<string> topKQueries(const vector<string>& log, int k) {
    if (k<=0) {
        return {};
    }
    unordered_map<string, int> fre;
    for(auto & query: log) {
        fre[query]++;
    }
    auto cmp =[](const pair<int, string> &a, const pair<int, string> &b) {
        if (a.first !=b.first) {
            return a.first > b.first;
        }
        return a.second < b.second;
    };
    priority_queue<pair<int, string>, vector<pair<int, string>>, decltype(cmp)>pq(cmp);
    for (auto & [query, cnt] : fre){
        if (pq.size() < static_cast<size_t>(k)) {
            pq.push({cnt, query});
            continue;
        }
        pair<int, string> ob ={cnt, query};
        if (cmp(ob, pq.top())) {
            pq.push({cnt, query});
            pq.pop();
        }
    }
    vector<string> ret;
    while (!pq.empty()) {
        ret.push_back(pq.top().second);
        pq.pop();
    }
    reverse(ret.begin(), ret.end());
    return ret;
}
// ============================

int main() {
    vector<string> log = {
        "dodd-frank whistleblower",
        "section 10b-5",
        "dodd-frank whistleblower",
        "erisa fiduciary duty",
        "section 10b-5",
        "dodd-frank whistleblower"
    };
    int k = 2;

    vector<string> got = topKQueries(log, k);

    vector<string> expected = {"dodd-frank whistleblower", "section 10b-5"};

    cout << "got:      ";
    for (auto& s : got) cout << "[" << s << "] ";
    cout << "\nexpected: ";
    for (auto& s : expected) cout << "[" << s << "] ";
    cout << "\n" << (got == expected ? "PASS" : "FAIL") << "\n";
    return 0;
}