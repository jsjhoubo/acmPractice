#include <iostream>
#include <string>
#include <vector>
using namespace std;

vector<string> parseCSVLine1(const string& line) {
    // TODO
    vector<string> ret;
    int i=0;
    string buffer;
    while (i<line.size()) {
        if (line[i] ==',') {
            if (buffer.size()>0 || (i>0 && line[i-1]==',')||(i==0)) {
                ret.push_back(buffer);
                buffer ="";
            }
            i++;
            continue;
        }
        if (line[i]=='\"') {
            buffer ="";
            i++;
            while (i<line.size() && !(line[i]=='"' && !(i+1<line.size() && line[i]=='"' && line[i+1]=='"'))) {
                if (line[i]=='"' && i+1<line.size() && line[i+1]=='"') {
                    buffer += '"';
                    i+=2;
                }
                else {
                    buffer += line[i];
                    i+=1;
                }
            }
            ret.push_back(buffer);
            buffer="";
            i+=1;
            
        }
        else {
            buffer += line[i];
            i++;
        }
    }
    if (buffer.size()>0) {
        ret.push_back(buffer);
    }
    else if (line.size()>0) {
        if (line[line.size()-1]==',') {
            ret.push_back(buffer);
        }
    }
    
    return ret;
}

vector<string> parseCSVLine(const string& line) {
    // TODO
    vector<string> ret;
    int i=0;
    string buffer;
    bool in_quota =false;
    while (i<line.size()) {
        if (line[i] ==',') {
            if (in_quota==true) {
                buffer +=',';
            }
            else {
                ret.push_back(buffer);
                buffer ="";    
            }
        }
        else if (line[i]=='"') {
            if (i+1 < line.size() && line[i+1]=='"') {
                buffer +='"';
                i++; // in end of loop it will increase one more time
            }
            else if (in_quota ==true) {
                in_quota =false;
            }
            else {
                in_quota =true;
            }
        }
        else {
            buffer +=line[i];
        }
        i++;
    }   
    if (line.size()>0) { 
        ret.push_back(buffer);
    }
    return ret;
}


void check(const string& name, const vector<string>& got, const vector<string>& exp) {
    bool ok = (got == exp);
    cout << (ok ? "[PASS] " : "[FAIL] ") << name;
    if (!ok) {
        cout << " | got: ";
        for (auto& s : got) cout << "[" << s << "]";
    }
    cout << "\n";
}

int main() {
    check("quoted",        parseCSVLine("\"a\",\"b\""),      {"a","b"});
    check("simple",        parseCSVLine("a,b,c"),            {"a","b","c"});
    check("empty fields",  parseCSVLine("a,,c"),             {"a","","c"});
   
    check("comma inside",  parseCSVLine("\"a,b\",c"),        {"a,b","c"});
    check("escaped quote", parseCSVLine("\"she said \"\"hi\"\"\""), {"she said \"hi\""});
    check("mixed",         parseCSVLine("a,\"b,c\",d"),      {"a","b,c","d"});
    check("trailing empty",parseCSVLine("a,b,"),             {"a","b",""});
    check("only commas",   parseCSVLine(",,"),               {"","",""});
}