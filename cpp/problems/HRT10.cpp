#include <cassert>
#include <iostream>
#include <string>
#include <vector>

using namespace std;
/*
// 开始行注释，直到该行结束
 开始块注释
 结束块注释
块注释可以跨多行
注释内部出现 //、/* 都没有特殊意义
空格 ' '、tab '\t' 不计入代码长度
其他字符都计数
假设输入中的块注释一定正确闭合
不考虑字符串字面量，也就是说 "//" 仍按注释开始处理
*/


// state
// 输入 lines
// 当前字符状态 is_comment：  1 表示在/**/注释中 ， 
// cnt code 总长度
// 算法
// 对于每一行 line
//   对于每个字符 line[i]
//       不是注释中，如果不是空格或者tab，cnt++
//                  如果line[i] =='/' i< line.size, line[i+1] ='/'  break
//            如果是/*  is_comment =1 
//       否则 line(i ..i+1) 是*/ is_comment =0
int codeLength(const vector<string>& lines) {
    // TODO
    int cnt =0;
    bool in_comment =false;
    for (auto & line : lines) {
        int i =0;
        // "int x = /* abc */ 10;"
        while (i < line.size()) {
            if (!in_comment) {
                if (line[i] =='/' && i+1 <line.size() && line[i+1] =='/') {
                    break;
                }
                if (line[i] =='/' && i+1 <line.size() && line[i+1] =='*') {
                    in_comment =true;
                    i =i+2;
                }
                else {
                    if (line[i] !=' ' && line[i]!='\t') {
                        cnt ++;
                    }
                    i =i+1;
                }
            }
            else {
                if (line[i] =='*' && i+1 <line.size() && line[i+1] =='/') {
                    in_comment =false;
                    i=i+2;
                }
                else {
                    i++;
                }
            }
        }
    }
    return cnt;
}

int main() {
    {
        vector<string> code = {
            "int x = 1;"
        };
        // intx=1; -> 7
        assert(codeLength(code) == 7);
    }

    {
        vector<string> code = {
            "int x = 1; // hello",
            "x++;"
        };
        // intx=1; + x++;
        assert(codeLength(code) == 11);
    }

    {
        vector<string> code = {
            "int x = /* abc */ 10;"
        };
        // intx=10;
        assert(codeLength(code) == 8);
    }

    {
        vector<string> code = {
            "int x = 1; /* start",
            "this is ignored",
            "still ignored */ x++;"
        };
        // intx=1; + x++;
        assert(codeLength(code) == 11);
    }

    {
        vector<string> code = {
            "abc/*xxx*/def//yyy",
            "ghi"
        };
        // abcdef + ghi
        assert(codeLength(code) == 9);
    }

    {
        vector<string> code = {
            "/* whole line */",
            "// another whole line",
            "   \t   "
        };
        assert(codeLength(code) == 0);
    }

    {
        vector<string> code = {
            "a/* // ignored",
            " /* still ignored */b"
        };
        // a + b
        assert(codeLength(code) == 2);
    }

    {
        vector<string> code = {
            "a// /* ignored",
            "b"
        };
        assert(codeLength(code) == 2);
    }

    {
        vector<string> code = {};
        assert(codeLength(code) == 0);
    }

    cout << "All tests passed!\n";
}