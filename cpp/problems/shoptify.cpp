#include <iostream>
#include <vector>
#include <map>
#include <algorithm>

using namespace std;
struct Item {
    long long id;
    long long price;
    size_t quantity;
    string category;
    vector<long long> product_level_discounts;
    long long getDiscountPrice() {
        if (product_level_discounts.size() ==0) {
            return price;
        }
        return product_level_discounts.back();
    }
};

struct Cart {
    vector<Item> products;
    long long total_price_after_discount;
    vector<string> discount_tracking;
    vector<long long> cart_level_discounts;
    void getDiscountPrice() {
        total_price_after_discount =0;
        for (int i=0;i<products.size();i++) {
            total_price_after_discount += products[i].getDiscountPrice() * products[i].quantity;
        }
        if (cart_level_discounts.size() !=0) {
            for(auto d: cart_level_discounts)
                total_price_after_discount -= d;
        }
    }
};
class Rule {
public:

    virtual void applyDiscount(Cart& cart) const =0;
    virtual ~Rule() =default;
};

class PercentOff: public Rule {
    string _category;
    double _percent;
public:
    PercentOff(string category, double percent): _category(category), _percent(percent){}
    void applyDiscount(Cart & cart) const override {
        for(auto & p : cart.products) {
            if (p.category ==_category) {
                cart.discount_tracking.push_back("product " + to_string(p.id) + " with from price " + to_string(p.price) + " have discount for category "
                 + _category + " by " + to_string(_percent) + "%");
                 long long tmp = p.product_level_discounts.size()==0 ? p.price : p.product_level_discounts.back();
                 
                 p.product_level_discounts.push_back((long long)(tmp * (1- _percent/100)));
            }
        }
    }
};

class SpendOver :public Rule {
private:
  long long _threshold;
  long long _minus;
public:
  SpendOver(long long threshold, long long minus) :_threshold(threshold), _minus(minus) {}
  void applyDiscount(Cart & cart) const override {
    cart.getDiscountPrice();
    if (cart.total_price_after_discount >= _threshold) {
        cart.cart_level_discounts.push_back(_minus);
        cart.discount_tracking.push_back("cart total price from " + to_string(cart.total_price_after_discount) +" to price "+
            to_string(cart.total_price_after_discount -_minus));
        cart.total_price_after_discount -= _minus;
    }
  }
};

class OverXGiftY : public Rule {
private:
    int _num_X;
    int _num_Y;
public:
    OverXGiftY(int num_X, int num_Y) : _num_X(num_X), _num_Y(num_Y) {}

    void applyDiscount(Cart & cart) const override {
        int total_item_size =cart.products.size();
        bool gift =false;
        long long min_price = LLONG_MAX;
        Item item;
        for (int i=0;i< total_item_size; i++) {
            auto & p =cart.products[i];
            if (p.quantity >= _num_X && p.price >0) {
                gift =true;
            }
            if (min_price > p.price && p.price !=0) {
                min_price = p.price;
                item.id = p.id;
                item.quantity =_num_Y;
                item.price =0;
                item.category =p.category;
            }
        }    
        if (gift) {
            cart.products.push_back(item);
            cart.discount_tracking.push_back("gift item Y " + to_string(item.id) + " duo to Over  X gift Y");
        }
    }
};


int main() {
    Cart cart{{{1, 100, 1, "book", {}}, {2, 50, 2, "book", {}}, {3, 200, 1, "toy", {}}}, 0, {}, {}};
    PercentOff rule("book", 10);   // book 减 10%
    rule.applyDiscount(cart);
    cart.getDiscountPrice();
    cout << "total: " << cart.total_price_after_discount << "\n";
    for (auto& s : cart.discount_tracking) cout << s << "\n";
}