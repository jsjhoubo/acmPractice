#include <unordered_map>
#include <string>
#include <list>

using namespace std;
class LFUCache {
public:
// state
// hashmap <fre, link<string>> m1
//    m1 每一个元素中的link 是lru 
//  capacity 目前大小
//  min_fre
//  hashmpa <string, <value, fre, link<string>::iter> > m2
// get:
//   如果 key 不存在，return -1
//   else 
//         val, cnt, iter=m2[key], 删掉m1[cnt]中的这个iter
//         m1[cnt+1].插入link 头 m2[key].iter 等于新iter
//         if m1[cnt].size ==0, delete m1[cnt] if min_fre ==cnt min_fre+1 
//         return val
/*
   put:      
         if m2 has key 
             val, cnt, iter = m2[key] 
             m1[cnt] delete iter
             if m1[cnt].size ==0, delete m1[cnt] if min_fre ==cnt min_fre+1
             m1[cnt+1] add to head of link
             m2[key] =val, cnt+1, new iter 
         else 
            如果 m2.size() = capacity
                m1[min_fre]的元素 队尾删除，删除对应m2中的元素，如果m1[min_fre]空，m1 delete min_fre
            min_fre =1
            key insert into head of m2[1]
            m2[key] = val, 1, m2[1].begin() 
        
        

*/       

    unordered_map<int, list<int>> m1;
//    m1 每一个元素中的link 是lru 
    int capacity;
    int min_fre;
    unordered_map<int, pair<int, list<int>::iterator> > m2;
    unordered_map<int, int> cache;
    LFUCache(int capacity) {
        capacity = capacity;
    }
    
    int get(int key) {
        if (cache.count(key) ==0) {
            return -1;
        }
        Delete(key);
        return cache[key];
    }
    
    void put(int key, int value) {
        if (cache.count(key) !=0) {
            Delete(key);
        }
        else {
            if (cache.size()==capacity) {
                int x = m1[min_fre].back();
                cache.erase(x);
                m1[min_fre].pop_back();
                if (m1[min_fre].size() ==0) {
                    m1.erase(min_fre);
                }
            }
            min_fre =1;
            m1[min_fre].push_front(key);
            m2[key] ={1, m1[min_fre].begin()};
        }
        cache[key] =value;
    }
private:
    void Delete(int key) {
        if (m2.count(key) ==0) {
            return;
        }
        auto [cnt, iter] =m2[key];
        m1[cnt].erase(iter);
        m1[cnt+1].push_front(key);
        m2[key] ={cnt +1, m1[cnt+1].begin()};
        if (m1[cnt].size()==0) {
            m1.erase(cnt);
            if (cnt ==min_fre) {
                min_fre ++;
            }
        }
    }
};

/**
 * Your LFUCache object will be instantiated and called as such:
 * LFUCache* obj = new LFUCache(capacity);
 * int param_1 = obj->get(key);
 * obj->put(key,value);
 */