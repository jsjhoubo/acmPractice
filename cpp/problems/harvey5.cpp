#include <iostream>
#include <vector>
#include <string>
#include <map>
using namespace std;

class FileSystem
{
    struct Node
    {
        string content;
        map<string, Node *> children;
        int is_dir =-1;
    };
    Node *r = new Node();
    vector<Node*>  ParsePath(const string &path, bool create_dir, bool create_file)
    {
        vector<Node*> nodes;
        int i = 0;
        string cur;

        auto node_opr = [&](Node *n, bool create_last)
        {
            if (create_dir)
            {
                if (n->children.count(cur) == 0)
                {
                    n->children[cur] = new Node();
                    if (!create_last)
                        n->children[cur]->is_dir =1;
                    else {
                        n->children[cur]->is_dir =0;
                    }
                }
                else {
                    if (n->children[cur]->is_dir ==0) {
                        throw runtime_error("create dir with none dir path " + path);
                    }
                }
            }
            else
            {
                if (n->children.count(cur) == 0)
                {
                    throw runtime_error("none existing path " + path);
                }
            }
        };
        Node *n = r;
        while (i < path.size())
        {
            nodes.push_back(n);
            if (path[i] == '/') {
                if (cur.size() > 0) {
                    node_opr(n, false);
                    n = n->children[cur];
                    cur = "";
                }
            }
            else {
                cur += path[i];
            }
            i++;
        }
        if (cur.size() > 0) {
            node_opr(n, create_file);
            n = n->children[cur];
            nodes.push_back(n);
        }
        return nodes;
    }

public:
    // create a directory; parents auto-created ("mkdir -p" semantics)
    void mkdir(const string &path)
    {
        auto nodes = ParsePath(path, true, false);
        if (nodes.size()==0) {
            throw runtime_error("path cannot create");
        }
    }
    // create or overwrite a file at path with content (string)
    void write(const string &path, const string &content) {
        vector<Node*> nodes =ParsePath(path, true, true);
        if (nodes.size()==0) {
            throw runtime_error("path cannot create");
        }
        nodes.back()->content =content;
    }
    // return file content; error if missing or path is a directory
    string Read(const string &path) {
        auto nodes =ParsePath(path, false, false);
        if (nodes.size()==0) {
            throw runtime_error("path cannot create");
        }
        if (nodes.back()->is_dir ==true) {
            throw runtime_error("read dir");
            
        }
        return nodes.back()->content;
    }
    // if path is a file → [its name] if path is a directory → sorted names of its children                
    vector<string> Ls(const string &path) {
        auto nodes =ParsePath(path, false, false);
        if (nodes.size()==0) {
            throw runtime_error("path cannot create");
        }
        vector<string> ret;
        auto n =nodes.back();
        if (!n->is_dir) {
            int i =path.size()-1;
            while (i>=0 && path[i]!='/') {
                i--;
            }
            ret.push_back(path.substr(i+1));
        } 
        else {
            for (auto & [k, v] : n->children) {
                ret.push_back(k);
            }
        }
        return ret;
    }
};

int main()
{
    FileSystem fs;

    fs.mkdir("/legal/contracts");
    fs.write("/legal/contracts/nda.txt", "confidential");
    fs.write("/legal/readme.md", "index");

    auto legal_list = fs.Ls("/legal");
    cout << "ls(/legal): [";
    for (size_t i = 0; i < legal_list.size(); ++i) {
        if (i > 0) {
            cout << ", ";
        }
        cout << legal_list[i];
    }
    cout << "]\n";

    auto file_list = fs.Ls("/legal/contracts/nda.txt");
    cout << "ls(/legal/contracts/nda.txt): [";
    for (size_t i = 0; i < file_list.size(); ++i) {
        if (i > 0) {
            cout << ", ";
        }
        cout << file_list[i];
    }
    cout << "]\n";

    cout << "read(/legal/contracts/nda.txt): "
         << fs.Read("/legal/contracts/nda.txt") << '\n';

    return 0;
}
