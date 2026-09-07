#include <bits/stdc++.h>
#include <functional>
using namespace std;

struct Node {
    int val;
    Node* prev;
    Node* next;
    Node* child;
    Node(int v): val(v), prev(nullptr), next(nullptr), child(nullptr) {}
};

// ============================================================
//  YOU IMPLEMENT THIS.
//  Flatten multilevel doubly linked list.
//  child inserted between node and node->next; all child ptrs
//  must be null afterward; prev/next fully rewired.
// ============================================================
Node* flatten(Node* head) {
    // TODO: your code here
    if (head==nullptr) {
        return nullptr;
    }

    function<Node*(Node*)> dfs =[&](Node* head) -> Node* {
        Node * p =head;
        Node * tail =p; 
        while (p!=nullptr) {
            Node* next =p->next;
            if (p->child !=nullptr) {
                Node * t = dfs(p->child);
                Node *c =p->child;
                p->child =nullptr;
                p->next =c;
                c->prev =p;
                t->next =next;
                if (next !=nullptr) {
                    next->prev =t;
                } 
                tail =t;
            }
            else {
                tail =p;
            }
            p =next;
        }
        return tail;
    };
    dfs(head);
    
    return head;
}

// ============================================================
//  Test harness — do not edit below.
// ============================================================
static int passed=0, failed=0;

// verify flattened list equals expected value order, AND
// next/prev are consistent, AND all child==nullptr.
static void check(const string& name, Node* head, vector<int> expected) {
    vector<int> fwd;
    Node* cur = head;
    Node* last = nullptr;
    bool childClean = true, prevOk = true;
    while (cur) {
        fwd.push_back(cur->val);
        if (cur->child != nullptr) childClean = false;
        if (cur->prev != last) prevOk = false;   // prev must point to previous node
        last = cur;
        cur = cur->next;
    }
    // also walk backward from last to check prev chain length matches
    vector<int> bwd;
    cur = last;
    while (cur) { bwd.push_back(cur->val); cur = cur->prev; }
    reverse(bwd.begin(), bwd.end());

    bool ok = (fwd == expected) && childClean && prevOk && (bwd == expected);
    if (ok) { passed++; printf("  [PASS] %s -> ", name.c_str()); }
    else    { failed++; printf("  [FAIL] %s\n", name.c_str());
              printf("         forward  ["); }
    if (ok) { printf("["); }
    for (size_t i=0;i<fwd.size();i++) printf("%d%s", fwd[i], i+1<fwd.size()?",":"");
    printf("]");
    if (!ok) {
        printf("\n         expected ["); for(size_t i=0;i<expected.size();i++) printf("%d%s",expected[i],i+1<expected.size()?",":""); printf("]");
        printf("\n         childClean=%s prevOk=%s backwardOk=%s", childClean?"Y":"N", prevOk?"Y":"N", (bwd==expected)?"Y":"N");
    }
    printf("\n");
}

// helpers to build
static Node* mklist(vector<int> vals) {
    Node* head=nullptr; Node* tail=nullptr;
    for(int v:vals){ Node* n=new Node(v); if(!head){head=tail=n;} else {tail->next=n; n->prev=tail; tail=n;} }
    return head;
}
static Node* at(Node* head, int idx){ while(idx-- && head) head=head->next; return head; }

int main() {
    // 1) no child: 1-2-3
    check("no_child", mklist({1,2,3}), {1,2,3});

    // 2) empty
    check("empty", nullptr, {});

    // 3) single node
    check("single", mklist({7}), {7});

    // 4) spec: 1-2-3-4-5-6 with child on 3 -> 7-8-9-10, child on 8 -> 11-12
    {
        Node* top = mklist({1,2,3,4,5,6});
        Node* mid = mklist({7,8,9,10});
        Node* bot = mklist({11,12});
        at(top,2)->child = mid;      // node 3 -> child 7
        at(mid,1)->child = bot;      // node 8 -> child 11
        check("spec_multilevel", top, {1,2,3,7,8,11,12,9,10,4,5,6});
    }

    // 5) child at head node
    {
        Node* top = mklist({1,2});
        Node* c = mklist({3,4});
        at(top,0)->child = c;        // node 1 -> child 3
        check("child_at_head", top, {1,3,4,2});
    }

    // 6) child at last node
    {
        Node* top = mklist({1,2});
        Node* c = mklist({3,4});
        at(top,1)->child = c;        // node 2 -> child 3
        check("child_at_tail", top, {1,2,3,4});
    }

    // 7) deep nesting: 1 -> child 2 -> child 3 -> child 4
    {
        Node* a=new Node(1); Node* b=new Node(2); Node* c=new Node(3); Node* d=new Node(4);
        a->child=b; b->child=c; c->child=d;
        check("deep_chain", a, {1,2,3,4});
    }

    // 8) child whose sublist also has a next after returning
    {
        Node* top = mklist({1,2,3});
        Node* c = mklist({4,5});
        at(top,0)->child = c;        // 1 -> child 4-5 ; expect 1,4,5,2,3
        check("child_reconnect", top, {1,4,5,2,3});
    }

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed==0 ? 0 : 1;
}