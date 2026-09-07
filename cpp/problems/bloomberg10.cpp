#include <bits/stdc++.h>
#include <unordered_map>
#include <vector>
using namespace std;

// ============================================================
//  YOU IMPLEMENT THIS.  All ops average O(1).
//    insert(val): insert if absent -> true; already present -> false
//    remove(val): remove if present -> true; absent -> false
//    getRandom(): return a uniformly random current element
// ============================================================
class RandomizedSet {
    vector<int> _buffer;
    unordered_map<int, int> _cache;
    int _current_size;
    int random_0_to_n_minus_1(int n) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(0, n - 1);
        return dist(gen);
    }
public:
    RandomizedSet() {
        // TODO
        _current_size =0;
    }
    bool insert(int val) {
        // TODO
        if (_cache.count(val) ==0) {
            _cache[val] = _current_size;
            if(_current_size == _buffer.size()) {
                _buffer.push_back(val);
            }
            else  {
                _buffer[_current_size] =val;
            }
            ++_current_size;
            return true;
        }
        return false;
    }
    bool remove(int val) {
        // TODO
        if (_cache.count(val) >0) {
            size_t ind = _cache[val];
            _buffer[ind] =_buffer.back();
            _cache[_buffer[ind]] =ind;
            -- _current_size;
            _cache.erase(val);
            return true;
        }
        return false;
    }
    int getRandom() {
        // TODO
        return _buffer[random_0_to_n_minus_1(_current_size)];
    }
};

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed=0, failed=0;
static void chk(const string& tag, bool got, bool exp) {
    if (got==exp) { passed++; printf("  [PASS] %s = %s\n", tag.c_str(), got?"true":"false"); }
    else { failed++; printf("  [FAIL] %s = %s, expected %s\n", tag.c_str(), got?"true":"false", exp?"true":"false"); }
}

int main() {
    {
        RandomizedSet s;
        chk("insert(1)", s.insert(1), true);
        chk("insert(1) dup", s.insert(1), false);
        chk("insert(2)", s.insert(2), true);
        chk("remove(1)", s.remove(1), true);
        chk("remove(1) again", s.remove(1), false);
        chk("remove(3) absent", s.remove(3), false);
        chk("insert(1) readd", s.insert(1), true);
        // after readd, set = {2,1}; getRandom must be 1 or 2
        for (int i=0;i<20;i++){ int r=s.getRandom(); if(r!=1&&r!=2){failed++;printf("  [FAIL] getRandom out of set: %d\n",r);} }
        printf("  [PASS] getRandom in-set spot check\n"); passed++;
    }
    {
        // single element: getRandom always that element
        RandomizedSet s; s.insert(42);
        bool ok=true; for(int i=0;i<50;i++) if(s.getRandom()!=42) ok=false;
        if(ok){passed++;printf("  [PASS] single_elem getRandom==42\n");} else {failed++;printf("  [FAIL] single_elem\n");}
    }
    {
        // remove middle then re-add many: stress the swap-remove path
        RandomizedSet s;
        for(int v=0;v<100;v++) s.insert(v);
        for(int v=0;v<100;v+=2) s.remove(v);   // remove evens
        // now only odds remain; verify membership via remove() semantics
        bool ok=true;
        for(int v=1;v<100;v+=2) if(!s.remove(v)) ok=false;  // all odds present -> true
        for(int v=0;v<100;v+=2) if(s.remove(v)) ok=false;    // evens absent -> false
        if(ok){passed++;printf("  [PASS] swap_remove_stress\n");} else {failed++;printf("  [FAIL] swap_remove_stress\n");}
    }
    {
        // uniformity: insert 5 elems, sample 100k, chi-square check
        RandomizedSet s;
        for(int v=0;v<5;v++) s.insert(v);
        map<int,int> cnt;
        int N=100000;
        for(int i=0;i<N;i++) cnt[s.getRandom()]++;
        double exp=N/5.0, chi=0;
        for(int v=0;v<5;v++){ double d=cnt[v]-exp; chi+=d*d/exp; }
        // df=4, chi-square 0.001 critical ~ 18.47; way above => non-uniform
        printf("  [INFO] uniformity counts:"); for(int v=0;v<5;v++) printf(" %d:%d",v,cnt[v]); printf("  chi2=%.2f\n",chi);
        if(chi < 18.47){passed++;printf("  [PASS] getRandom uniform (chi2<18.47)\n");}
        else {failed++;printf("  [FAIL] getRandom NON-uniform (chi2=%.2f)\n",chi);}
    }

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed==0 ? 0 : 1;
}