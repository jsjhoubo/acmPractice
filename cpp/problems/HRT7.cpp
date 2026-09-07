#include <cassert>
#include <iostream>
#include <vector>
#include <stack>
using namespace std;

struct Node {
    int val;
    Node* left;
    Node* right;

    Node(int v) : val(v), left(nullptr), right(nullptr) {}
};
/*作，所以node，先入栈，左子树如果存在，再入栈，
这样保证先入栈的是后入栈的孩子，那么loop invariant是？ 
关键怎么直到左右子树都已经访问完了，最开始的时候root 入栈，进入loop，
对于栈顶元素而言，看上一个输出的元素，如果是左孩子，说明左子树访问完毕，
如果是右孩子说明右子树访问完毕，这个时候退栈打印这个元素，如果都不是那么看左右孩子
，存在则入栈，不存在则出栈。 用一个例子验证下，
 [1] [1 2] [1 2 4] [1 2] 4 [1 2 5] 4 [1 2] 4 5 [1] 4 5 2 [1 3] 4 5 2 [1 ] 4 5 2 3 [] 4 5 2 3 1*/
vector<int> postorder(Node* root) {
    if (root ==nullptr) {
        return {};
    }

    stack<Node*> st;
    Node * last =nullptr; // record last element of post order
    st.push(root);
    vector<int> ret;
        //       1
        //      / \
        //     2   3
        //    / \
        //   4   5
    //st =[1] last =null, ret={}
    //  t = 1 [1 2]
    // t =2  last =null ret ={} [1 2 4]
    // t =4  ret =[4] last =4  [1 2]
    // t =2  ret =[4] last =4 [1 2 5]
    // t =5  ret =[4 5] last =5 [1 2]
    // t =2  ret = [4 5 2] last =2 [1]
    // t =1  ret =[4 5 2] last =2 [1 3]
    // t =3  ret =[4 5 2 3] last =3 [1]
    // t =1  
    while (!st.empty()) {
        Node * t = st.top();
        if ((t->left ==nullptr && t->right ==nullptr) || 
            (last !=nullptr && t->right == nullptr && t->left ==last) ||
            (last !=nullptr && t->right != nullptr && t->right ==last)) {
            ret.push_back(t->val);
            last =t;
            st.pop();
            continue;
        }
        if (t->left !=nullptr && last!=t->left) {
            st.push(t->left);
        } 
        else if (t->right !=nullptr && (
            t->left == nullptr ||
          (t->left !=nullptr && t->left == last) 
        )) {
            st.push(t->right);
        }
    }
    return ret;
}

int main() {
    {
        Node* root = nullptr;
        assert(postorder(root) == vector<int>{});
    }

    {
        Node a(1);
        assert(postorder(&a) == vector<int>{1});
    }

    {
        //       1
        //      / \
        //     2   3
        //    / \
        //   4   5
        Node n1(1), n2(2), n3(3), n4(4), n5(5);
        n1.left = &n2;
        n1.right = &n3;
        n2.left = &n4;
        n2.right = &n5;

        assert(postorder(&n1) ==
               vector<int>({4,5,2,3,1}));
    }

    {
        // 1
        //  \
        //   2
        //    \
        //     3
        Node n1(1), n2(2), n3(3);
        n1.right = &n2;
        n2.right = &n3;

        assert(postorder(&n1) ==
               vector<int>({3,2,1}));
    }

    {
        //     1
        //    /
        //   2
        //  /
        // 3
        Node n1(1), n2(2), n3(3);
        n1.left = &n2;
        n2.left = &n3;

        assert(postorder(&n1) ==
               vector<int>({3,2,1}));
    }

    cout << "All tests passed!\n";
}