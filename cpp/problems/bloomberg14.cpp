#include <bits/stdc++.h>
#include <functional>
using namespace std;

struct TreeNode
{
    int val;
    TreeNode *left, *right;
    TreeNode(int v) : val(v), left(nullptr), right(nullptr) {}
};

// ============================================================
//  YOU IMPLEMENT BOTH.
// ============================================================

// ---- (1) GENERAL binary tree (not BST) ----
//  Hint to self: must encode nulls explicitly (preorder + "#").
class CodecGeneral
{
public:
    string serialize(TreeNode *root)
    {
        // TODO
        string str;
        function<void(TreeNode *)> dfs = [&](TreeNode *r)
        {
            if (r == nullptr)
            {
                str += "#";
                return;
            }
            str += to_string(r->val) + " ";
            dfs(r->left);
            str += " ";
            dfs(r->right);
            str += " ";
        };
        dfs(root);
        return str;
    }
    TreeNode *deserialize(string data)
    {
        // TODO
        function<TreeNode *(size_t &)> dfs = [&](size_t &start) -> TreeNode *
        {
            if (start == data.size())
            {
                return nullptr;
            }
            while (start < data.size() && data[start] == ' ')
            {
                ++start;
            }
            if (start < data.size() && data[start] == '#')
            {
                ++start;
                return nullptr;
            }

            int val = 0;
            int sign = 1;
            int x = start;
            if (start < data.size() && data[start] == '-')
            {
                sign = -1;
                ++start;
            }
            while (start < data.size() && data[start] >= '0' && data[start] <= '9')
            {
                val = val * 10 + data[start] - '0';
                ++start;
            }
            TreeNode *n = nullptr;
            if (start > x)
            {
                val = val * sign;
                n = new TreeNode(val);
                n->left = dfs(start);
                n->right = dfs(start);
            }
            return n;
        };
        size_t start = 0;
        TreeNode *r = dfs(start);
        return r;
    }
};

// ---- (2) BST ----
//  Hint to self: preorder only, no null markers; rebuild via value-range bounds.
class CodecBST
{
public:
    string serialize(TreeNode *root)
    {
        // TODO
        string str;
        function<void(TreeNode *)> dfs = [&](TreeNode *r)
        {
            if (r == nullptr)
            {
                return;
            }
            str += to_string(r->val) + " ";
            if (r->left != nullptr)
            {
                dfs(r->left);
                str += " ";
            }
            if (r->right != nullptr)
            {
                dfs(r->right);
                str += " ";
            }
        };
        dfs(root);
        return str;
    }
    TreeNode *deserialize(string data)
    {
        // TODO
        TreeNode *root = nullptr;
        function<TreeNode *(size_t &, int, int)> dfs =
            [&](size_t &start, int mmin, int mmax) -> TreeNode *
        {
            if (start == data.size())
            {
                return nullptr;
            }
            while (start < data.size() && data[start] == ' ')
            {
                ++start;
            }

            int val = 0;
            int sign = 1;
            int x = start;

            if (start < data.size() && data[start] == '-')
            {
                sign = -1;
                ++start;
            }

            while (start < data.size() &&
                   data[start] >= '0' && data[start] <= '9')
            {
                val = val * 10 + data[start] - '0';
                ++start;
            }

            TreeNode *n = nullptr;
            if (start > x)
            {
                val = val * sign;
                n = new TreeNode(val);

                if (val > mmax)
                {
                    start =x;
                    return nullptr;
                }
                if (val < mmin)
                {
                    start =x;
                    return nullptr;
                }

                n->left = dfs(start, mmin, val);
                n->right = dfs(start, val, mmax);
            }

            return n;
        };
        size_t start = 0;
        root = dfs(start, INT_MIN, INT_MAX);
        return root;
    }
};

// ============================================================
//  Test harness — do not edit below.
//  Runs the SAME trees through BOTH codecs.
//  (General codec must handle any tree; BST codec is only fed BST-valid trees.)
// ============================================================
static int passed = 0, failed = 0;
static bool sameTree(TreeNode *a, TreeNode *b)
{
    if (!a && !b)
        return true;
    if (!a || !b)
        return false;
    return a->val == b->val && sameTree(a->left, b->left) && sameTree(a->right, b->right);
}
static TreeNode *mk(int v) { return new TreeNode(v); }
static void insertBST(TreeNode *&root, int v)
{
    if (!root)
    {
        root = new TreeNode(v);
        return;
    }
    if (v < root->val)
        insertBST(root->left, v);
    else
        insertBST(root->right, v);
}
static TreeNode *buildBST(vector<int> vals)
{
    TreeNode *r = nullptr;
    for (int v : vals)
        insertBST(r, v);
    return r;
}

template <class Codec>
static void chk(const string &tag, const string &name, TreeNode *root)
{
    Codec c;
    string s = c.serialize(root);
    TreeNode *back = c.deserialize(s);
    bool ok = sameTree(root, back);
    bool stable = (c.serialize(back) == s);
    if (ok && stable)
    {
        passed++;
        printf("  [PASS] %-8s %-14s \"%s\"\n", tag.c_str(), name.c_str(), s.c_str());
    }
    else
    {
        failed++;
        printf("  [FAIL] %-8s %-14s \"%s\" (sameTree=%d stable=%d)\n", tag.c_str(), name.c_str(), s.c_str(), ok, stable);
    }
}

int main()
{
    // ---- General codec: any binary tree ----
    printf("=== CodecGeneral (any tree) ===\n");
    chk<CodecGeneral>("GEN", "empty", nullptr);
    chk<CodecGeneral>("GEN", "single", mk(1));
    {
        TreeNode *r = mk(1);
        r->left = mk(2);
        r->right = mk(3);
        r->right->left = mk(4);
        r->right->right = mk(5);
        chk<CodecGeneral>("GEN", "basic", r);
    }
    {
        TreeNode *r = mk(1);
        r->left = mk(2);
        r->left->left = mk(3);
        r->left->left->left = mk(4);
        chk<CodecGeneral>("GEN", "left_skew", r);
    }
    {
        TreeNode *r = mk(-100);
        r->left = mk(1000000000);
        r->right = mk(-7);
        chk<CodecGeneral>("GEN", "neg_multi", r);
    }
    {
        TreeNode *r = mk(5);
        r->left = mk(3);
        r->left->right = mk(4);
        chk<CodecGeneral>("GEN", "one_child", r);
    }

    // ---- BST codec: BST-valid trees only ----
    printf("\n=== CodecBST (BST only) ===\n");
    chk<CodecBST>("BST", "empty", nullptr);
    chk<CodecBST>("BST", "single", buildBST({5}));
    chk<CodecBST>("BST", "balanced", buildBST({5, 3, 7, 2, 4, 6, 8}));
    chk<CodecBST>("BST", "left_skew", buildBST({5, 4, 3, 2, 1}));
    chk<CodecBST>("BST", "right_skew", buildBST({1, 2, 3, 4, 5}));
    chk<CodecBST>("BST", "neg_vals", buildBST({0, -5, 5, -10, -2}));
    chk<CodecBST>("BST", "multidigit", buildBST({500, 100, 900, 50, 1000000000}));

    printf("\n==== %d passed, %d failed ====\n", passed, failed);
    return failed == 0 ? 0 : 1;
}