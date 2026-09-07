#include <cstdio>
#include <unordered_map>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <iostream>

using namespace std;

class SpreadSheet {
    unordered_map<string, int> _cache;
public:
    void Set(const string & key, int value) {
        _cache[key] =value;
    }
    int Get(const string & key) {
        if (_cache.count(key) ==0) {
            return 0;
        }
        return _cache[key];
    }
};

struct CellContent {
    int val =0;
    int valid =false;
    vector<string> depends;
    void SetVal(int val) {
        this->val =val;
        this->valid =true;
    }
    void SetDepends(vector<string> & depends) {
        this->depends = vector<string>(depends);
        this->valid =false;
    }
};

// . set("A1",1), set("B2",2), sum("D1", ["A1:B2","B2"]), get("D1") —
// A1:1
// B2:2
// sum D1:{A1:B2","B2"}
// D1, valid =false, check range
// r1,c1 =1,1
// r2,c2 =2,2
// get 3
// then for B2, direct get 2
class SpreadSheetV1 {
    unordered_map<string, CellContent> _cache;
    pair<int, int> ParseCellCoordinate(const string & cell) {
        // parse 
        int r =0;
        int c =0;
        int i=0;
        while (i<cell.size()) {
            if (cell[i] >='0' && cell[i] <='9') {
                r =r*10 + cell[i]-'0';
            }
            else if (cell[i] >='A' && cell[i] <='Z'){
                c =c*26 + cell[i] -'A' +1;
            }
            i++;
        }
        return {r,c};
    }
    string Coordinate2Str(int r, int c) {
        //
        string r_str =to_string(r);
        int t =c;
        string c_str ="";
        while (t>0) {
            int m =t%26;
            if (m==0) {
                c_str += 'Z';
            }
            else 
                c_str += (m-1 +'A');
            t =t/26-1;
        } 
        reverse(c_str.begin(),c_str.end());
        return c_str + r_str;
    }
    int GetHelper(const string & cell) {
        if (_cache.count(cell) ==0) {
            return 0;
        }
        if (_cache[cell].valid) {
            return _cache[cell].val;
        }
        int total =0;
        for (auto & range : _cache[cell].depends) {
            int index = range.find(":");
            if (index == string::npos) {
                if (_cache.count(range) !=0) {
                    total += GetHelper(range);
                }
            }
            else {
                auto[r1, c1] =ParseCellCoordinate(range.substr(0, index)) ;
                auto[r2, c2] =ParseCellCoordinate(range.substr(index+1)) ;
                for (int r =r1;r<=r2;r++) {
                    for (int c =c1;c<=c2;c++) {
                        string name = Coordinate2Str(r, c);
                        if (_cache.count(name) ==0) {
                            continue;
                        }
                        total += GetHelper(name);
                    }
                }
              
            }
        }
        return total;
    }

    unordered_map<string, vector<string>> _graph;

    bool HasCycle(const string & node, const string & target, unordered_set<string> & vis) {
        vis.insert(node);
        if (_graph.count(node) >0) {
            for (auto & next : _graph[node]) {
                if (next ==target) {
                    return true;
                }
                if (vis.count(next) ==0) {
                    bool flag = HasCycle(next, target, vis);
                    if (flag) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

public: 
    void Set(const string & cell, int value) {
        _cache[cell].SetVal(value);
    }
    int Get(const string & cell) {
        return GetHelper(cell);
    }

    void sum(const string & target_cell, vector<string> & cells) {
        if (cells.size() ==0) {
            return;
        }
        bool has_old =false;
        CellContent old_content;
        vector<string> old_nodes;
        if (_cache.count(target_cell)>0) {
            old_content =_cache[target_cell];
            has_old =true;
            old_nodes = _graph[target_cell];
        }
        bool has_cycle =false;
        unordered_set<string> vis;
        for (auto & range : cells) {
            int index = range.find(":");
            if (index == string::npos) {
                    _graph[target_cell].push_back(range);
                    vis.clear();
                    has_cycle = HasCycle(target_cell, target_cell, vis);
                
            }
            else {
                auto[r1, c1] =ParseCellCoordinate(range.substr(0, index)) ;
                auto[r2, c2] =ParseCellCoordinate(range.substr(index+1)) ;
                for (int r =r1;r<=r2;r++) {
                    for (int c =c1;c<=c2;c++) {
                        string name = Coordinate2Str(r, c);
                        _graph[target_cell].push_back(name);
                        vis.clear();
                        has_cycle = HasCycle(target_cell, target_cell, vis);
                        if (has_cycle) {
                            break;
                        }
                    }
                    if (has_cycle) {
                        break;
                    }
                }
            }
            if (has_cycle) {
                break;
            }
        }
        if (!has_cycle) {
            _cache[target_cell].SetDepends(cells);
            cout << " the target cell sum has cycle dependence" <<endl;
        }
        else {
            if (has_old) {
                _cache[target_cell] =old_content;
                _graph[target_cell] =old_nodes;
            }
            else {
                // no need to keep in graph
                _graph.erase(target_cell);
            }
        }
    }
};

int main () {
    return 0;
}

