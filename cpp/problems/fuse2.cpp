#include <vector>
#include <algorithm>
#include <string>
#include <functional>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <queue>
#include <cassert> 

using namespace std;

struct Coordinate {
    int row;
    int col;
    string name;
    Coordinate(const string & cell) {
        size_t i =0;
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
        name = to_string(row) + ":" + to_string(col);
    }
    Coordinate(int row, int col) {
        this->row =row;
        this->col =col;
        name = to_string(row) + ":" + to_string(col);
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
        if (from == to) {
            return true;
        }
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
    struct Range {
        Coordinate start;
        Coordinate end;
    };

    unordered_map<int, unordered_map<int, Cell>> _cache;
    Graph _graph;
    unordered_map<string, vector<Range>> _sum_ranges;

    template <typename Func>
    void ForEachDependencyCell(const vector<Coordinate> & dep, Func && func) {
        if (dep.size() == 1) {
            func(dep[0]);
            return;
        }
        for (int r = dep[0].row; r <= dep[1].row; r++) {
            for (int c = dep[0].col; c <= dep[1].col; c++) {
                func(Coordinate(r, c));
            }
        }
    }

    bool Exist(const Coordinate & co) {
        if (_cache.count(co.row) ==0) {
            return false;
        }
        if (_cache[co.row].count(co.col) ==0) {
            return false;
        }
        return true;
    }

    bool ParseName(const string & name, Coordinate & out) {
        size_t p = name.find(':');
        if (p == string::npos) return false;
        out = Coordinate(stoi(name.substr(0, p)), stoi(name.substr(p + 1)));
        return true;
    }

    bool InRange(const Coordinate & p, const Coordinate & start, const Coordinate & end) {
        return start.row <= p.row && p.row <= end.row && start.col <= p.col && p.col <= end.col;
    }

    void AttachCoverageEdges(const Coordinate & co, bool check_cycle, bool & has_cycle, vector<string> & attached_targets) {
        for (const auto & [target, ranges] : _sum_ranges) {
            if (target == co.name) {
                continue;
            }
            bool covered = false;
            for (const auto & rg : ranges) {
                if (InRange(co, rg.start, rg.end)) {
                    covered = true;
                    break;
                }
            }
            if (!covered) {
                continue;
            }
            if (check_cycle && _graph.AddEdgeHasCycle(co.name, target)) {
                has_cycle = true;
                return;
            }
            _graph.addEdges(co.name, target);
            attached_targets.push_back(target);
        }
    }

    void InvalidateDownstream(const string & source) {
        unordered_set<string> vis;
        queue<string> q;
        vis.insert(source);
        q.push(source);
        while (!q.empty()) {
            string cur = q.front(); q.pop();
            if (_graph.data.count(cur) == 0) continue;
            for (const auto & nxt : _graph.data[cur]) {
                if (!vis.insert(nxt).second) continue;
                q.push(nxt);
                if (_sum_ranges.count(nxt)) {
                    Coordinate co(0, 0);
                    if (ParseName(nxt, co) && Exist(co)) _cache[co.row][co.col].valid = false;
                }
            }
        }
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
        for (const auto & dep : cell_content.depends) {
            ForEachDependencyCell(dep, [&](const Coordinate & co) {
                if (Exist(co)) {
                    val += GetHelper(co);
                }
            });
        }
        cell_content.val = val;
        cell_content.valid = true;
        return val;
    }

    void RemoveEdgeHelper(const Coordinate & target_co) {
        if (Exist(target_co) ==false) {
            return ;
        }
        Cell & cell_content =_cache[target_co.row][target_co.col];
        
        for (const auto & dep : cell_content.depends) {
            ForEachDependencyCell(dep, [&](const Coordinate & co) {
                _graph.removeEdges(co.name, target_co.name);
            });
        }
    }
    
    void addEdgeHelper(const Coordinate & target_co, bool & has_cycle) {
        if (Exist(target_co) ==false) {
            return ;
        }
        Cell & cell_content =_cache[target_co.row][target_co.col];
        vector<pair<string, string>> added_edges;
        for (const auto & dep : cell_content.depends) {
            ForEachDependencyCell(dep, [&](const Coordinate & co) {
                if (has_cycle) {
                    return;
                }
                if (Exist(co)) {
                    bool flag = _graph.AddEdgeHasCycle(co.name, target_co.name);
                    if (flag) {
                        has_cycle = true;
                        return;
                    }
                    _graph.addEdges(co.name, target_co.name);
                }
                added_edges.push_back({co.name, target_co.name});
            });
            if (has_cycle) {
                break;
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
        if (Exist(co) && _sum_ranges.count(co.name)) {
            RemoveEdgeHelper(co);
            _sum_ranges.erase(co.name);
        }
        Cell cell_content(value);
        _cache[co.row][co.col] =cell_content;
        bool has_cycle = false;
        vector<string> attached_targets;
        AttachCoverageEdges(co, false, has_cycle, attached_targets);
        InvalidateDownstream(co.name);
    }
    int Get(const string & cell) {
        Coordinate co(cell);
        return GetHelper(co);
    }
    void Sum(const string & target_cell, vector<string> & cells) {
        Coordinate co(target_cell);
        Cell old_cell_content;
        bool has_content =false;
        bool old_is_sum = _sum_ranges.count(co.name) > 0;
        vector<Range> old_ranges = old_is_sum ? _sum_ranges[co.name] : vector<Range>();
        if (Exist(co)) {
            has_content =true;
            old_cell_content =_cache[co.row][co.col];
            if (old_is_sum) RemoveEdgeHelper(co);
        }
        _sum_ranges[co.name].clear();
        Cell cell_content;
        for (auto  & cell : cells) {
            size_t index =cell.find(':');
            vector<Coordinate> dep;
            
            if (index ==string::npos) {
                dep.push_back(Coordinate(cell));
                _sum_ranges[co.name].push_back({dep[0], dep[0]});
            }
            else {
                dep.push_back(Coordinate(cell.substr(0, index)));
                dep.push_back(Coordinate(cell.substr(index+1)));
                _sum_ranges[co.name].push_back({dep[0], dep[1]});
            }
            cell_content.depends.push_back(dep);
        }
        _cache[co.row][co.col] =cell_content;
        bool has_cycle =false;
        vector<string> attached_targets;
        AttachCoverageEdges(co, true, has_cycle, attached_targets);
        if (!has_cycle) {
            addEdgeHelper(co, has_cycle);
        }
        if (has_cycle) {
            for (const auto & target : attached_targets) {
                _graph.removeEdges(co.name, target);
            }
        }
        if (has_content && has_cycle) {
            _cache[co.row][co.col] =old_cell_content;
            if (!old_is_sum) _sum_ranges.erase(co.name);
            else _sum_ranges[co.name] = old_ranges;
            has_cycle =false;
            if (old_is_sum) addEdgeHelper(co, has_cycle);
        }
        if (has_cycle && has_content ==false) {
            _cache[co.row].erase(co.col);
            _sum_ranges.erase(co.name);
        } 
        if (!has_cycle) {
            _cache[co.row][co.col].valid = false;
            InvalidateDownstream(co.name);
        }
    }
};

int main (){
    {
        SpreadSheetV1 sheet;
        vector<string> d1 = {"A1"};
        sheet.Sum("D1", d1);
        vector<string> a1 = {"D1"};
        sheet.Sum("A1", a1);
        assert(sheet.Get("A1") == 0);
    }
    {
        SpreadSheetV1 sheet;
        vector<string> f1 = {"F2"};
        sheet.Sum("F1", f1);
        vector<string> f2 = {"A1"};
        sheet.Sum("F2", f2);
        assert(sheet.Get("F1") == 0);
        sheet.Set("A1", 5);
        assert(sheet.Get("F1") == 5);
    }
    return 0;
}