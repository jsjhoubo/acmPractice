#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
using namespace std;

/*
dot: .
horizontal stick: ---
vertical stick: |
空位用空格
*/


/*
state:
  point {(r, c), adject points, valid function, is_adject func, add_ajeents}

  (r-1)*9+(c-1) => hash

  hash{r,c} =point
  vector<point> graph
  limit =9
algorihtm
 add:
   points(x1, y1) not valid return false
   points(x2, y2) not valid return false
   manhadun distance (x1,y1) (x2, y2) >1 return false
   x1 ,y1 adjetns no x2 y2 else return fasle
   x2 y2 no adject x1 y1 esle return false
   then add ajecent each other
   return true

 done:
   dfs helper, 
     recursive invariant
       given point, depth, target
         if (depth ==3 and point.is_aj(target)) 
           return true
         else if (depth >=3) {
         retun false
        }
       check adject points of given point
    for each points in graph
    
  play layout:
    strinng buf two demisnion (9 + 8) *(9+8)
     point i, j
      in (i-1)*2 (j-1)*2
    for i=1;i<=9;i++
       
       for j=1; j<=9;j++
         set (.)

    for all points in graph:
       check adjects in graph 
         r1, c1, r2 c2
           r1 ==r2 , add -- betwen (c1-2) *2 (c2-1)*2
           c1 == c2 add | betwwen (r1-2) *2 (r2-1) *2
         
  

*/

/*
H[9][8] = horizontal sticks
V[8][9] = vertical sticks

addStick:
- validate
- normalize orientation
- check duplicate
- set one bool

done:
- 扫 8x8 个 cell
- 检查四条边

printLayout:
- dots + H + V 映射到字符网格*/
class Game {
    vector<vector<bool>> _H;
    vector<vector<bool>> _V;
    int _n;

    bool Adjcent(int x1, int y1, int x2 ,int y2) const {
        // implement it later
        return false;
    }

    void addEdge(int x1, int y1, int x2 ,int y2) {

    }
 public:
    Game(int n) {
        // TODO
        _n =n;
        _H =vector<vector<bool>>(n, vector<bool>(n-1, false)); // one line one row,
        _V =vector<vector<bool>>(n, vector<bool>(n-1, false)); // one column one line
    }

    // Add one unit-length horizontal/vertical stick.
    // Return false if:
    // - coordinate out of [1,9]
    // - points are not adjacent
    // - stick already exists
    bool addStick(int x1, int y1, int x2, int y2) {
        // TODO
        if (abs(x1 -x2) + abs(x2-y2) !=1) {
            return false;
        }
        auto valid =[&](int x, int y) {return x>=1 && x<=_n && y>=1 && y<=_n}; 
        
        if (!valid(x1, y1)) return false;
        if (!valid(x2, y2)) return false;
        int dir[4][2] ={{-1,0}, {1,0}, {0,-1},{0,1}};
        auto duplicate_edge =[&](int c, int r, int tc, int tr) {
            for (int i=0;i<4;i++) {
                int r1 =r + dir[i][0];
                int c1 =c + dir[i][1];
                if (valid(r1,c1)) {
                    if (Adjcent(c, r, r1, c1) && r1==tr && c1==tc) {
                        return true;
                    }
                }
            }
            return false;
        };
        if (duplicate_edge(x1, y1, x2, y2)) {
            return false;
        }
        
        return true;
    }

    // Return true iff at least one 1x1 square is completed.
    bool done() const {
        // TODO
        for (int i=0;i<_n-1;i++) {
            for (int j=0;j<_n-1;j++) {
                if (Adjcent(i, j, i+1, j) && Adjcent(i, j, i, j+1) &&  
                 Adjcent(i+1, j, i+1, j+1) && Adjcent(i, j+1, i+1, j+1) ) {
                    return true;
                 }
            }
        }
        return false;
    }

    // Print current board to stdout.
    //
    // '.'   = dot
    // '---' = horizontal stick
    // '|'   = vertical stick
    void printLayout() const {
        vector<vector<char>> buffer(_n*2-1, vector<char>(_n*2-1, '.'));
        for (int i=1;i<=_n-1;i++) {
            for (int j=1;j<=_n-1;j++) {
                if (Adjcent(i, j, i, j+1)) {
                    // column i, row j
                  int c =(j-1) * 2 +1;
                  int r =(i-1) * 2 +1;
                  buffer[c][r] = '|';

                }
                if (Adjcent(i, j, i+1, j)) {
                  int c =(j-1) * 2 +1;
                  int r =(i-1) * 2 +1;
                  buffer[c][r] = '-';
                }
            }
        }
    }
};

int main() {
    {
        Game g;

        // empty board: no square
        assert(g.done() == false);

        // valid sticks
        assert(g.addStick(1,1,1,2) == true);
        assert(g.addStick(1,1,2,1) == true);

        // duplicate
        assert(g.addStick(1,1,1,2) == false);

        // reversed duplicate
        assert(g.addStick(1,2,1,1) == false);

        // invalid: diagonal
        assert(g.addStick(2,2,3,3) == false);

        // invalid: same dot
        assert(g.addStick(2,2,2,2) == false);

        // invalid: too far
        assert(g.addStick(1,1,1,3) == false);

        // invalid: out of range
        assert(g.addStick(0,1,1,1) == false);
        assert(g.addStick(9,9,9,10) == false);

        assert(g.done() == false);
    }

    {
        Game g;

        // complete square with corners:
        //
        // (1,1) --- (1,2)
        //   |           |
        // (2,1) --- (2,2)

        assert(g.addStick(1,1,1,2) == true); // top
        assert(g.done() == false);

        assert(g.addStick(1,1,2,1) == true); // left
        assert(g.done() == false);

        assert(g.addStick(2,1,2,2) == true); // bottom
        assert(g.done() == false);

        assert(g.addStick(1,2,2,2) == true); // right
        assert(g.done() == true);
    }

    {
        Game g;

        // square somewhere in the middle
        assert(g.addStick(5,5,5,6));
        assert(g.addStick(5,5,6,5));
        assert(g.addStick(6,5,6,6));

        assert(g.done() == false);

        assert(g.addStick(5,6,6,6));
        assert(g.done() == true);
    }

    {
        Game g;

        // boundary square at bottom-right
        assert(g.addStick(8,8,8,9));
        assert(g.addStick(8,8,9,8));
        assert(g.addStick(9,8,9,9));
        assert(g.addStick(8,9,9,9));

        assert(g.done() == true);
    }

    {
        // Optional visual test
        Game g;
        g.addStick(1,1,1,2);
        g.addStick(1,1,2,1);
        g.addStick(2,1,2,2);
        g.addStick(1,2,2,2);

        g.printLayout();
    }

    cout << "All tests passed!\n";
}