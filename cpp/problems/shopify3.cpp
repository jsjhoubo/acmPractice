#include <iostream>
#include <string>
#include <unordered_map>
using namespace std;

class VendingMachine {
    struct Product { int price; int qty; };
    unordered_map<string, Product> _products;
    int _inserted = 0;   // 当前已投金额
public:
    void add_product(const string& name, int price, int qty) {
        Product p;
        p.price = price;
        p.qty =qty;
        _products[name] =p;
    }
    void insert_coin(int amount) {
        _inserted += amount;
    }
    // 返回值约定：出货成功返回找零额；售罄/不存在返回退款额；钱不够返回 0
    int select(const string& name) {
        if (_products.count(name) ==0 || _products[name].qty ==0) {
            return refund();
        }
        if (_products[name].price > _inserted) {
            return 0;
        }
        _inserted -=_products[name].price;
        _products[name].qty -=1;
        int tmp =_inserted;
        _inserted =0;
        return tmp;
    }
    int refund() {
        if (_inserted > 0) {
            int tmp =_inserted;
            _inserted =0;
            return tmp;
        }
        return 0;
    }
    // 测试辅助：暴露内部状态查询
    int inserted() const { return _inserted; }
    int stock(const string& name) {
        return _products.count(name) ? _products[name].qty : -1;
    }
};

void check(const string& name, bool ok) {
    cout << (ok ? "[PASS] " : "[FAIL] ") << name << "\n";
}

int main() {
    // 1. 正常购买 + 找零
    { VendingMachine m; m.add_product("coke", 150, 2);
      m.insert_coin(100); m.insert_coin(100);          // 投了 200
      int change = m.select("coke");
      check("buy coke change=50", change==50);
      check("coke stock now 1", m.stock("coke")==1);
      check("inserted cleared", m.inserted()==0); }

    // 2. 钱不够
    { VendingMachine m; m.add_product("coke", 150, 2);
      m.insert_coin(100);
      int r = m.select("coke");
      check("not enough returns 0", r==0);
      check("money still held", m.inserted()==100);    // 钱还在
      check("stock unchanged", m.stock("coke")==2); }

    // 3. 售罄 → 退款
    { VendingMachine m; m.add_product("coke", 150, 0);  // 库存 0
      m.insert_coin(200);
      int r = m.select("coke");
      check("sold out refunds 200", r==200);
      check("inserted cleared after refund", m.inserted()==0); }

    // 4. 商品不存在 → 退款
    { VendingMachine m; m.insert_coin(100);
      int r = m.select("water");
      check("no product refunds 100", r==100); }

    // 5. 主动退款
    { VendingMachine m; m.insert_coin(75);
      int r = m.refund();
      check("refund returns 75", r==75);
      check("inserted cleared", m.inserted()==0); }

    // 6. 正好够钱（边界：inserted == price）
    { VendingMachine m; m.add_product("gum", 50, 1);
      m.insert_coin(50);
      int change = m.select("gum");
      check("exact money change=0", change==0);
      check("gum stock 0", m.stock("gum")==0); }
}