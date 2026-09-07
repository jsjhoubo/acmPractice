#include <unordered_map>
#include <string>

using namespace std;
#include <unordered_map>
#include <string>

using namespace std;
class FileSystem {
private:
    struct File {
        unordered_map<string, File> children;
        int value;
    };
    bool parse(const string & path, bool MiddleMustExist, bool finalMustExist, int & value) {
        unordered_map<string, File> *cur_pointer = &root;
        if (path.size()==0) {
            return false;
        }
        if (path =="/") {
            return false;
        }
        if (path[0]!='/') {
            return false;
        }
        int i =0;
        int j =1;
        i++;
        while (i < path.size()) {
            // if meet start of next filename in path
            if (path[i]=='/') {
                if (j ==i) {
                    return false;
                }
                // /a/ j =1, i=2
                string name = path.substr(j, i-j);
                // not last filename
                auto cur =*cur_pointer;
                if (MiddleMustExist) {
                    if (cur.count(name) ==0) {
                        return false;
                    }
                } else {
                    cur[name] ={{},-1};
                }          
                j =i+1;
                cur_pointer =&(cur[name].children);
            }
            else if (!(path[i]>='a' && path[i]<='z')) {
                return false;
            }
            i++;
        }
        if (j ==i) {
            return false;
        }
        string name =path.substr(j, i-j);
        auto cur =* cur_pointer;
        if (finalMustExist) {
            if (cur.count(name) ==0) {
                return false;
            }
            value =cur[name].value;
        }
        else {
            if (cur.count(name) !=0) {
                return false;
            }
            cur[name] ={{}, value};
        }
        return true;
    }
public:
    FileSystem() {
        
    }

    unordered_map<string, File> root;
    
    bool createPath(string path, int value) {
        int val =value;
        bool flag =parse(path, true, false, val);
        
        if (flag ==false) {
            return false;
        }
        return true;
    }
    
    int get(string path) {
        int val =-1;
        bool flag =parse(path, true, true, val);
        if (flag ==false) {
            return -1;
        }
        return val;
    }
};

/**
 * Your FileSystem object will be instantiated and called as such:
 * FileSystem* obj = new FileSystem();
 * bool param_1 = obj->createPath(path,value);
 * int param_2 = obj->get(path);
 */

/**
 * Your FileSystem object will be instantiated and called as such:
 * FileSystem* obj = new FileSystem();
 * bool param_1 = obj->createPath(path,value);
 * int param_2 = obj->get(path);
 */