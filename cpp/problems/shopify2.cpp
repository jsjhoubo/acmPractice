#include <iostream>
#include <string>
#include <unordered_map>
using namespace std;

class Rover {
    int _x, _y, _w, _h;
    char _dir;
public:
    Rover(int x,int y,char dir,int w,int h):_x(x),_y(y),_dir(dir),_w(w),_h(h){}
    void execute(const string& cmds) {
        // TODO
        auto valid =[&](int x, int y) {
            return x>=0 && x<_w && y >=0 && y<_h;
        };
        unordered_map<char, char> L ={{'N', 'W'}, {'W', 'S'}, {'S', 'E'}, {'E', 'N'}};
        unordered_map<char, char> R ={{'N', 'E'}, {'E', 'S'}, {'S', 'W'}, {'W', 'N'}};
        auto move =[](char dir, int &x, int &y){
            if (dir =='N') {
                y +=1;
            }
            else if (dir =='S') {
                y -=1;
            }
            else if (dir =='W') {
                x -=1;
            }
            else if (dir =='E') {
                x+=1;
            }
        };

        
        for (int i=0;i<cmds.size();i++) {
            int x1 =_x;
            int y1 =_y;
            char dir =_dir;
            if (cmds[i]=='M') {
               move(dir, x1, y1); 
            }
            else if (cmds[i]=='L'){
                dir =L[dir];
            }
            else {
                dir =R[dir];
            }
            if (valid(x1, y1)) {
                _x =x1;
                _y =y1;
                _dir =dir;
            }
        }
    }
    int x()const{return _x;} int y()const{return _y;} char dir()const{return _dir;}
};

// 一个小辅助：打印结果
void check(const string& name, bool ok) {
    cout << (ok ? "[PASS] " : "[FAIL] ") << name << "\n";
}

int main() {
    { Rover r(0,0,'N',5,5); r.execute("R"); check("turn right", r.dir()=='E'); }
    { Rover r(0,0,'N',5,5); r.execute("L"); check("turn left",  r.dir()=='W'); }
    { Rover r(0,0,'N',5,5); r.execute("M"); check("move north", r.x()==0 && r.y()==1); }
    { Rover r(1,2,'N',5,5); r.execute("RMM"); check("combo RMM", r.x()==3 && r.y()==2 && r.dir()=='E'); }
    { Rover r(0,0,'N',5,5); r.execute("RRRR"); check("full circle", r.dir()=='N'); }
    { Rover r(0,0,'S',5,5); r.execute("M"); check("hit bottom wall", r.x()==0 && r.y()==0); }
    { Rover r(4,4,'N',5,5); r.execute("M"); check("hit top wall", r.x()==4 && r.y()==4); }
}