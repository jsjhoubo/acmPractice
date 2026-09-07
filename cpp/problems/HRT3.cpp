#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>
#include <ranges>

using namespace std;

enum class Side {
    Buy,
    Sell
};

struct Order {
    int id;
    int size;
    int price;
    Side side;
};

struct Fill {
    int orderId;
    int size;
    int price;

    bool operator==(const Fill& other) const {
        return orderId == other.orderId &&
               size == other.size &&
               price == other.price;
    }
};

class Auction {
public:
    void addOrder(const Order& order) {
        orders_.push_back(order);
    }

    vector<Fill> matchOrders() {
        // TODO: implement
        sort(orders_.begin(), orders_.end(), [](const Order & a, const Order &b) {
            if (a.price != b.price)
                return a.price < b.price;
            return a.side == Side::Sell && b.side == Side::Buy;
        });

        int n = orders_.size();
        if (n ==0) {
            return vector<Fill>();
        }
        vector<int> buy_accumulate(n, 0);
        
        for (int i=orders_.size()-1;i>=0;i--) {
            if (i<orders_.size()-1) {
                buy_accumulate[i] =buy_accumulate[i+1];
            }
            if (orders_[i].side == Side::Buy) {
                buy_accumulate[i] += orders_[i].size;
            }
        }
        int sell_total =0;
        int max_size =0;
        int p =0;
        for (int i=0;i<orders_.size();i++) {
            if (orders_[i].side == Side::Sell) {
                sell_total += orders_[i].size;
            }
            int val =min(sell_total, buy_accumulate[i]);
            if (val > max_size) {
                max_size =val;
                p =orders_[i].price;
            }
        }
        int cnt =max_size;
        vector<Fill> ret;
        for (auto & order : orders_) {
            if (order.price > p) {
                break;
            }
            if (order.side == Side::Sell) {
                int amount = cnt >= order.size? order.size : cnt;
                ret.emplace_back(Fill{order.id, amount, p});
                cnt -= order.size;
                if (cnt <=0 ){
                    break;
                }
            }
        }
        cnt =max_size;
        for (auto & order : orders_ | std::views::reverse) {
            if (order.price < p) {
                break;
            }
            if (order.side == Side::Buy) {
                int amount = cnt >= order.size? order.size : cnt;
                ret.emplace_back(Fill{order.id, amount, p});
                cnt -= order.size;
                if (cnt <=0 ){
                    break;
                }
            }
        }
        return ret;
    }

private:
    vector<Order> orders_;
};

static void sortFills(vector<Fill>& fills) {
    sort(fills.begin(), fills.end(),
         [](const Fill& a, const Fill& b) {
             return a.orderId < b.orderId;
         });
}

static void check(
    vector<Fill> actual,
    vector<Fill> expected
) {
    sortFills(actual);
    sortFills(expected);

    assert(actual == expected);
}

int main() {
    // Test 1: example
    {
        Auction auction;

        auction.addOrder({1, 100, 10, Side::Buy});
        auction.addOrder({2, 150,  8, Side::Sell});
        auction.addOrder({3, 200,  8, Side::Buy});

        check(
            auction.matchOrders(),
            {
                {1, 100, 8},
                {2, 150, 8},
                {3,  50, 8}
            }
        );
    }

    // Test 2: no possible trade
    {
        Auction auction;

        auction.addOrder({1, 100, 5, Side::Buy});
        auction.addOrder({2, 100, 8, Side::Sell});

        check(
            auction.matchOrders(),
            {}
        );
    }

    // Test 3: partial fill on sell side
    {
        Auction auction;

        auction.addOrder({1, 100, 10, Side::Buy});
        auction.addOrder({2,  50,  6, Side::Sell});
        auction.addOrder({3, 100,  7, Side::Sell});

        check(
            auction.matchOrders(),
            {
                {1, 100, 7},
                {2,  50, 7},
                {3,  50, 7}
            }
        );
    }

    // Test 4: price priority on buy side
    {
        Auction auction;

        auction.addOrder({1, 100, 10, Side::Buy});
        auction.addOrder({2, 100, 12, Side::Buy});
        auction.addOrder({3, 120,  8, Side::Sell});

        check(
            auction.matchOrders(),
            {
                {2, 100, 8},
                {1,  20, 8},
                {3, 120, 8}
            }
        );
    }

    // Test 5: multiple sells, lower sell price first
    {
        Auction auction;

        auction.addOrder({1, 150, 10, Side::Buy});
        auction.addOrder({2, 100,  5, Side::Sell});
        auction.addOrder({3, 100,  7, Side::Sell});

        check(
            auction.matchOrders(),
            {
                {1, 150, 7},
                {2, 100, 7},
                {3,  50, 7}
            }
        );
    }

    // Test 6: empty auction
    {
        Auction auction;

        check(
            auction.matchOrders(),
            {}
        );
    }

    // Test 7: one-sided auction
    {
        Auction auction;

        auction.addOrder({1, 100, 10, Side::Buy});
        auction.addOrder({2, 100, 12, Side::Buy});

        check(
            auction.matchOrders(),
            {}
        );
    }

    cout << "All tests passed!\n";
}