#include <iostream>
#include <vector>
#include <unordered_map>
using namespace std;

class Inventory {
    unordered_map<long long, int> _stock;
    unordered_map<long long, int> _stock_reserved;
    struct Order {
        long long order_id;
        vector<pair<long long, int> >items;
        bool reserve_success;
        bool confirm;
        Order() {
            reserve_success =false;
            confirm =false;
        }
    };
    unordered_map<long long, Order> _orders;

public:
    void add_stock(long long product_id, int qty) {
        // — 进货,增加某商品库存 
        if (qty <0) {
            cout << "error qty " <<qty <<" is negative or zero" <<endl;
            return;
        }
        _stock[product_id] += qty;
    }
    bool reserve(long long order_id, vector<pair<long long, int> >items) {
    //  — 一个订单预订多个商品(items = 一组 (product_id, qty))。约定:all-or-nothing——只要有任一商品库存不足,整个预订失败、不改动任何库存,
        if (_orders.find(order_id) != _orders.end()) {
            cout << " order id " << order_id <<" already exists" <<endl;
            return false;
        }
        Order order;
        order.order_id =order_id;
        order.items =items;
        _orders[order_id] =order;
        bool success =true;
        for (auto [id, cnt] : items) {
            int x =available(id);
            if (x ==-1) {
                cout << " order id " << order_id <<" product id "<< id <<" is not in stock"<< endl;
                success =false;
                continue;
            }
            if (x < cnt) {
                cout << " order id " << order_id <<" product id "<< id <<" number" <<cnt <<" require bigger than "<<x <<" in stock"<< endl;
                success =false;
            }
        }
        if (success ==false) {
            return false;
        }
        _orders[order_id].reserve_success =true;
        for (auto [id, cnt]: items) {
            _stock_reserved[id] +=cnt;
        }
        return true;
    }

    bool release(long long order_id) { 
        //— 取消订单,把它预订但未确认的库存释放回去
        if (_orders.count(order_id) ==0) {
         cout <<" order " << order_id << " does not in the system"<<endl;
         return false;
        }
        if (_orders[order_id].confirm ) {
         cout <<" order " << order_id << " does confrim in the system and cannot release"<<endl;
         return false;
        }
        if (_orders[order_id].reserve_success ==false ) {
         cout <<" order " << order_id << " does reserve success"<<endl;
         return false;
        }
        cout <<"order " <<order_id <<" released";
        _orders[order_id].reserve_success =false;
        for (auto [id, cnt]: _orders[order_id].items) {
            _stock_reserved[id] -=cnt;
        }
        return true;
     }
    bool confirm(long long order_id) {
        //  — 订单支付成功,预订的库存实际扣除(从此不可 release)
       if (_orders.count(order_id) ==0) {
         cout <<" order " << order_id << " does not in the system"<<endl;
         return false;
       }
       if (!_orders[order_id].reserve_success ) {
         cout <<" order " << order_id << " does not reserve success"<<endl;
         return false;
       }
       if (_orders[order_id].confirm ) {
         cout <<" order " << order_id << " does confrim in the system"<<endl;
         return false;
       }
       _orders[order_id].confirm =true;
       for (auto [id, cnt] : _orders[order_id].items) {
        _stock[id] -=cnt;
        _stock_reserved[id] -=cnt;
        if (_stock_reserved[id] ==0) {
            _stock_reserved.erase(id);
        }
        if (_stock[id] ==0) {
            _stock.erase(id);
        }
       }
       return true;
    }
    int available(long long product_id) {
        //— 返回可再预订的库存量
        // return negative if no items availible
        auto it = _stock.find(product_id);
        int cnt =0;
        if (_stock_reserved.find(product_id) !=_stock_reserved.end()) {
            cnt =_stock_reserved[product_id];
        }
        if (it != _stock.end()) {
            return it->second-cnt;
        }
        return -1;
    }
};

int main() {
    return 0;
}