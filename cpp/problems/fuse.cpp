#include <vector>
#include <algorithm>
#include <string>
#include <unordered_map>
#include <functional>
using namespace std;

class SpreadSheet {
    unordered_map<string, int> _cache;
public:
    void set(string & cell, int value) {
        _cache[cell] =value;
    }
    int get(string & cell) {
        if (_cache.count(cell) ==0) {
            return 0;
        }
        return _cache[cell];
    }
};

class SpreadSheetV1 {
 // convert each cell name to column number and row number
    struct Coordernate {
        int row;
        int col;
    };
    struct Cell {
        int val =0;
        vector<pair<string, vector<pair<Coordernate, Coordernate>>>> ops;
    };
    unordered_map<int, unordered_map<int, Cell>> _cache;
    Coordernate GetRowCol(string & cell) {
        int i =0;
        int col =0;
        int row =0;
        while (i<cell.size()) {
            if (cell[i]>='A' && cell[i] <='Z') {
                col = col * 26 + (cell[i] -'A' +1);
            }
            else if (cell[i] >='0' && cell[i] <='9'){
                row = row * 10 + cell[i] -'0';
            }
            i++;
        }
        Coordernate co;
        co.row =row;
        co.col =col;
        return co;
    }
public:
    void set(string & cell, int value) {
        Coordernate co = GetRowCol(cell);
        Cell ce;
        ce.val = value;
        _cache[co.row][co.col] =ce;
    }
    int get(string & cell) {
        Coordernate co = GetRowCol(cell);
        

        function<int(Coordernate)> helper = [&](Coordernate t) ->int{
            if (_cache.count(t.row) ==0) {
                return 0;
            }
            if (_cache[t.row].count(t.col) ==0) {
                return 0;
            }
            auto & content =_cache[t.row][t.col];
            if (content.ops.size() == 0)  {
                return content.val;
            }
            int val = 0;
            for (int i=0;i<content.ops.size();i++) {
                if (content.ops[i].first=="sum") {
                    for (int j=0;j<content.ops[i].second.size();j++) {
                        auto x = content.ops[i].second[j];
                        for (int r =x.first.row;r<=x.second.row;r++) {
                            for (int c =x.first.col;c<=x.second.col;c++) {
                                Coordernate t1;
                                t1.row =r;
                                t1.col =c;
                                int sub_val =helper(t1);
                                val += sub_val;
                            }
                        }
                    }
                }
            }
            return val;
        };
        int val =helper(co);
        return val;
    }

    void sum(string &target_cell, vector<string> & cells) {
         Coordernate target_co = GetRowCol(target_cell);
         _cache[target_co.row][target_co.col]={};
         _cache[target_co.row][target_co.col].ops.push_back({"sum", vector<pair<Coordernate, Coordernate>>()});
         for (int i=0;i<cells.size();i++) {
            int j =cells[i].find(':');
            Coordernate co1;
            Coordernate co2;
            if (j !=string::npos) {
                string first =cells[i].substr(0, j);
                string second =cells[i].substr(j+1);
                co1 = GetRowCol(first);
                co2 = GetRowCol(second);
            }
            else {
                co1 = GetRowCol(cells[i]);
                co2.col =co1.col;
                co2.row =co1.row;
            }
            _cache[target_co.row][target_co.col].ops.back().second.push_back({co1, co2});
         }
    }
};
int main() {
    return 0;
}