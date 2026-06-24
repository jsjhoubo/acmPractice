#include <iostream>
#include <vector>
#include <map>
#include <queue>
using namespace std;

class RevertIndexManage {
private:
    map<string, vector<long long>> _cache;
    vector<long long> kWayMerge(vector<vector<long long>> id_lists) {
        auto cmp =[&](const pair<size_t, size_t> & a, const pair<size_t, size_t> & b) {
            return id_lists[a.first][a.second] > id_lists[b.first][b.second];
        };
        vector<long long> ret;
        priority_queue<pair<size_t, size_t>, vector<pair<size_t, size_t>>, decltype(cmp)> pq(cmp);
        for (size_t i=0;i< id_lists.size();i++) {
            pq.push({i, 0});
        }
        long long id =-1;
        size_t cnt =0;
        while (!pq.empty()) {
            auto [ind1, ind2] =pq.top();
            pq.pop();
            long long val =id_lists[ind1][ind2];
            if (id ==-1) {
                id =id_lists[ind1][ind2];
            }
            if (val == id) {
                cnt ++;
            }
            else {
                cnt =1;
                id =val;
            }
            if(cnt == id_lists.size()) {
                ret.push_back(id);
            }
            if (ind2+1 < id_lists[ind1].size()) {
                pq.push({ind1, ind2+1});
            }
        }
        return ret;
    }

    void check(string name, vector<long long> got, vector<long long> exp){
        cout << name << "  got=[";
        for(long long x:got) cout<<x<<" ";
        cout << "] exp=[";
        for(long long x:exp) cout<<x<<" ";
        cout << "]  " << (got==exp?"PASS":"FAIL") << "\n";
    }

public:
    void testKwayMerge() {
        check("three_lists", kWayMerge({{1,2,4,5},{2,3},{2,4,5}}), {2});
        // 你给的法律例子： dodd-frank ∩ whistleblower ∩ sec = {4,9,12}
        check("legal", kWayMerge({{1,4,7,9,12,20},{4,9,12,15},{2,4,9,12,18}}), {4,9,12});
        // early-stop：有一个 list 很短且很早耗尽
        check("early_stop", kWayMerge({{5},{5,100,200,300},{5,7,9}}), {5});
        // 单 term → 原样返回
        check("single", kWayMerge({{3,6,9}}), {3,6,9});
    }

    RevertIndexManage(map<string, vector<long long>> cache) :_cache(cache) {

    }

    vector<long long> findDocIds(string & query) {
        if (query.size() ==0) {
            return {};
        }
        vector<string> tokens;
        auto parseQuery =[&]() {
          size_t i=0;
          size_t j=0;
          while (i< query.size()) {
            while (i<query.size() && query[i] ==' ') {
                i++;
            }
            j =i;
            while (i<query.size() && query[i] !=' ') {
                i++;
            }
            if (i>j) {
                tokens.push_back(query.substr(j, i-j));
            }
            j=i;
          }  
        };
        parseQuery();
        vector<vector<long long>> id_lists;
        for (auto token : tokens) {
            auto iter =_cache.find(token); 
            if (iter ==_cache.end()) {
                return {};
            }
            if (iter->second.size() ==0) {
                return {};
            }
            id_lists.push_back(iter->second);
        }

        return kWayMerge(id_lists);
    }
};

int main() {
    // ---- kWayMerge unit tests ----
    RevertIndexManage re({});
    re.testKwayMerge();

    // ---- findDocIds tests ----
    map<string, vector<long long>> idx = {
        {"dodd-frank",    {1, 4, 7, 9, 12, 20}},
        {"whistleblower", {4, 9, 12, 15}},
        {"sec",           {2, 4, 9, 12, 18}},
        {"erisa",         {3, 5, 9}},
        {"fiduciary",     {5, 9, 21}},
    };
    RevertIndexManage rim(idx);

    auto check = [](const string& name, vector<long long> got, vector<long long> exp) {
        cout << name << "  got=[";
        for (long long x : got) cout << x << " ";
        cout << "] exp=[";
        for (long long x : exp) cout << x << " ";
        cout << "]  " << (got == exp ? "PASS" : "FAIL") << "\n";
    };

    // single term
    string q1 = "sec";
    check("single_term",       rim.findDocIds(q1), {2, 4, 9, 12, 18});

    // two-term intersection
    string q2 = "dodd-frank whistleblower";
    check("two_term",          rim.findDocIds(q2), {4, 9, 12});

    // three-term intersection
    string q3 = "dodd-frank whistleblower sec";
    check("three_term",        rim.findDocIds(q3), {4, 9, 12});

    // term not in index → empty
    string q4 = "dodd-frank unknown";
    check("missing_term",      rim.findDocIds(q4), {});

    // no common doc → empty
    string q5 = "erisa sec";
    check("no_intersection",   rim.findDocIds(q5), {9});

    // empty query → empty
    string q6 = "";
    check("empty_query",       rim.findDocIds(q6), {});

    // extra spaces between tokens
    string q7 = "  erisa  fiduciary  ";
    check("extra_spaces",      rim.findDocIds(q7), {5,9});

    return 0;
}