#include <vector>
#include <iostream>
#include <algorithm>
#include <limits>
#include <random>
#include <functional>
#include <cfloat>
using namespace std;

struct Node {
    double score;
    std::vector<Node*> children;
    Node(double s) : score(s) {}
};

double maxRootToLeafScore(Node* root) {
    if (root==nullptr) {
        return 0;
    }
    double ret =-DBL_MAX;
    std::function<void(Node*, double)> dfs =[&](Node*r, double cur) {
        if (r->children.size()==0) {
            ret =max(ret, cur + r->score);
            return;
        }
        for (int i=0;i<r->children.size();i++) {
            dfs(r->children[i], cur + r->score);
        }
    };
    dfs(root, 0);
    return ret;
}


double bruteForce(Node* root) {
    if (!root) return 0.0;
    if (root->children.empty()) return root->score;
    double best = -std::numeric_limits<double>::infinity();
    for (Node* c : root->children) {
        best = max(best, bruteForce(c));
    }
    return root->score + best;
}

const double EPS = 1e-9;

void runCase(const string& name, Node* root) {
    double got  = maxRootToLeafScore(root);
    double want = bruteForce(root);
    bool ok = (root == nullptr) ? (got == want) : (fabs(got - want) < EPS);
    cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name
         << "  got=" << got << " want=" << want << "\n";
}

Node* buildTree(const vector<double>& scores,
                const vector<pair<int,int>>& edges) {
    if (scores.empty()) return nullptr;
    vector<Node*> nodes;
    for (double s : scores) nodes.push_back(new Node(s));
    for (auto& e : edges) nodes[e.first]->children.push_back(nodes[e.second]);
    return nodes[0];
}

int main() {
    runCase("null tree", nullptr);

    runCase("single node", new Node(5.0));

    {
        // 0 -> 1, 0 -> 2 ; leaf scores: 1=3, 2=10
        // root 0 = 1.0 ; best path 0->2 = 11.0
        Node* r = buildTree({1.0, 3.0, 10.0}, {{0,1},{0,2}});
        runCase("two children", r);
    }

    {
        // negative scores: 0=-1, 1=-5, 2=-2 ; both leaves negative
        // 0->2 = -3 better than 0->1 = -6
        Node* r = buildTree({-1.0, -5.0, -2.0}, {{0,1},{0,2}});
        runCase("all negative", r);
    }

    {
        //        0(2)
        //       /    \
        //     1(3)   2(1)
        //     /  \      \
        //   3(1) 4(8)   5(10)
        // paths: 0-1-3=6, 0-1-4=13, 0-2-5=13
        Node* r = buildTree({2,3,1,1,8,10},
                            {{0,1},{0,2},{1,3},{1,4},{2,5}});
        runCase("deeper tree", r);
    }

    {
        // skewed chain: 0->1->2->3, scores 1,2,3,4 -> 10
        Node* r = buildTree({1,2,3,4}, {{0,1},{1,2},{2,3}});
        runCase("chain", r);
    }

    {
        // mixed sign, deeper
        // 0=5, 1=-10, 2=3 ; 1 has child 3=20 ; 2 has child 4=1
        // 0->1->3 = 15 ; 0->2->4 = 9
        Node* r = buildTree({5,-10,3,20,1},
                            {{0,1},{0,2},{1,3},{2,4}});
        runCase("mixed sign deep", r);
    }

    // random oracle
    std::mt19937 rng(7);
    std::uniform_real_distribution<double> sc(-10, 10);
    std::uniform_int_distribution<int> childDist(0, 3);

    int trials = 2000, passed = 0;
    for (int t = 0; t < trials; ++t) {
        int total = 1 + (rng() % 30);
        vector<Node*> nodes;
        for (int i = 0; i < total; ++i)
            nodes.push_back(new Node(round(sc(rng)*10)/10.0));
        // 每个非根节点随机挂到一个更小 index 的节点下 (保证是树/森林, 取 0 为根)
        for (int i = 1; i < total; ++i) {
            int parent = rng() % i;
            nodes[parent]->children.push_back(nodes[i]);
        }
        Node* root = nodes[0];
        double got  = maxRootToLeafScore(root);
        double want = bruteForce(root);
        if (fabs(got - want) < EPS) ++passed;
        else {
            cout << "[FAIL] random trial " << t
                 << " got=" << got << " want=" << want << "\n";
        }
    }
    cout << "random oracle: " << passed << "/" << trials << " passed\n";

    return 0;
}