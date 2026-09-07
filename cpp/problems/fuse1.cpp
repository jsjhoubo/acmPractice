#include <vector>
#include <algorithm>
#include <string>
#include <functional>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <cassert> 

using namespace std;

class SpreadSheet {
    unordered_map<string, int> _cache;
public:
    void set(const string & cell, int value) {
        _cache[cell] =value;
    }
    int get(const string & cell) {
        if (_cache.count(cell) ==0) {
            return 0;
        }
        return _cache[cell];
    }
};

struct Coordinate {
    int row;
    int col;
    string name;
    Coordinate(const string & cell) {
        name =cell;
        int i =0;
        row =0;
        col =0;
        while (i<cell.size()) {
            if (cell[i]>='A' && cell[i] <='Z') {
                col = col * 26 + (cell[i] -'A' +1);
            }
            else if (cell[i] >='0' && cell[i] <='9'){
                row = row * 10 + cell[i] -'0';
            }
            i++;
        }
    }
    Coordinate(int row, int col) {
        this->row =row;
        this->col =col;
        // to do generate name
    }
};

struct Cell {
    int val =0;
    bool valid =false;
    vector<vector<Coordinate>> depends;
    Cell() {

    }  
    Cell(int val) {
        this->val =val;
        this->valid =true;
    }
};

struct Graph {
    unordered_map<string, unordered_set<string>> data;
    void addEdges(const string & from, const string & to) {
        data[from].insert(to);
    }

    void removeEdges(const string & from, const string & to) {
        if (data.count(from) ==0) {
            return;
        }
        data[from].erase(to);
    }

    bool AddEdgeHasCycle(const string & from, const string & to) {
        unordered_set<string> visited;
        visited.insert(to);
        function<bool(const string &)> dfs = [&](const string &r) ->bool {
            if (data.count(r) ==0) {
                return false;
            }
            for (auto & node : data[r]) {
                if (node ==from) {
                    return true;
                }
                if (visited.count(node)==0) {
                    visited.insert(node);
                    bool flag =dfs(node);
                    if (flag) {
                        return true;
                    }
                }
            }
            return false;
        };
        return dfs(to);
    }
};

class SpreadSheetV1 {
    unordered_map<int, unordered_map<int, Cell>> _cache;
    Graph _graph;

    bool Exist(const Coordinate & co) {
        if (_cache.count(co.row) ==0) {
            return false;
        }
        if (_cache[co.row].count(co.col) ==0) {
            return false;
        }
        return true;
    }

    int GetHelper(const Coordinate & target_co) {
        if (Exist(target_co) ==false) {
            return 0;
        }
        Cell & cell_content =_cache[target_co.row][target_co.col];
        if (cell_content.valid) {
            return cell_content.val;
        }
        int val =0;
        for (auto dep : cell_content.depends) {
            if (dep.size() ==1) {
                if (!Exist(dep[0])) {
                    continue;
                }
                val += GetHelper(dep[0]);
            }
            else {
                for (int r =dep[0].row;r<=dep[1].row;r++) {
                    for (int c =dep[0].col;c<=dep[1].col;c++) {
                        Coordinate co(r, c);
                        if (!Exist(co)) {
                            continue;
                        }
                        val += GetHelper(co);
                    }
                }
            }
        }
        return val;
    }

    void RemoveEdgeHelper(const Coordinate & target_co) {
        if (Exist(target_co) ==false) {
            return ;
        }
        Cell & cell_content =_cache[target_co.row][target_co.col];
        
        for (auto dep : cell_content.depends) {
            if (dep.size() ==1) {
                if (!Exist(dep[0])) {
                    continue;
                }
                _graph.removeEdges(target_co.name, dep[0].name);
            }
            else {
                for (int r =dep[0].row;r<=dep[1].row;r++) {
                    for (int c =dep[0].col;c<=dep[1].col;c++) {
                        Coordinate co(r, c);
                        if (!Exist(co)) {
                            continue;
                        }
                        _graph.removeEdges(target_co.name, co.name);
                    }
                }
            }
        }
    }
    
    void addEdgeHelper(const Coordinate & target_co, bool & has_cycle) {
        if (Exist(target_co) ==false) {
            return ;
        }
        Cell & cell_content =_cache[target_co.row][target_co.col];
        vector<pair<string, string>> added_edges;
        for (auto dep : cell_content.depends) {
            if (dep.size() ==1) {
                if (!Exist(dep[0])) {
                    continue;
                }
                bool flag =_graph.AddEdgeHasCycle(target_co.name, dep[0].name);
                if (flag) {
                    has_cycle =true;
                    break;
                }
                _graph.addEdges(target_co.name, dep[0].name);
                added_edges.push_back({target_co.name, dep[0].name});
            }
            else {
                for (int r =dep[0].row;r<=dep[1].row;r++) {
                    for (int c =dep[0].col;c<=dep[1].col;c++) {
                        Coordinate co(r, c);
                        if (!Exist(co)) {
                            continue;
                        }
                        bool flag =_graph.AddEdgeHasCycle(target_co.name, co.name);
                        if (flag) {
                            has_cycle =true;
                            break;
                        }
                        _graph.addEdges(target_co.name, co.name);
                        added_edges.push_back({target_co.name, co.name});
                    }
                }
                if (has_cycle) {
                    break;
                }
            }
        }
        if (has_cycle) {
            for (auto & [from, to] : added_edges) {
                _graph.removeEdges(from, to);
            }
        }
    }
public:
    void Set(const string & cell, int value) {
        Coordinate co(cell);
        Cell cell_content(value);
        _cache[co.row][co.col] =cell_content;
    }
    int Get(const string & cell) {
        Coordinate co(cell);
        return GetHelper(co);
    }
    void Sum(const string & target_cell, vector<string> & cells) {
        Coordinate co(target_cell);
        Cell old_cell_content;
        bool has_content =false;
        if (Exist(co)) {
            has_content =true;
            old_cell_content =_cache[co.row][co.col];
            RemoveEdgeHelper(co);
        }
        Cell cell_content;
        for (auto  & cell : cells) {
            int index =cell.find(':');
            vector<Coordinate> dep;
            
            if (index ==string::npos) {
                dep.push_back(Coordinate(cell));
            }
            else {
                dep.push_back(Coordinate(cell.substr(0, index)));
                dep.push_back(Coordinate(cell.substr(index+1)));
            }
            cell_content.depends.push_back(dep);
        }
        _cache[co.row][co.col] =cell_content;
        bool has_cycle =false;
        addEdgeHelper(co, has_cycle);
        if (has_content && has_cycle) {
            _cache[co.row][co.col] =old_cell_content;
            has_cycle =false;
            addEdgeHelper(co, has_cycle);
        }
    }
};

int main (){
    return 0;
}