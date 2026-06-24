#include <iostream>
#include <atomic>
#include <algorithm>

class MarketRadar {
public:
    MarketRadar() : _max_price(0) {}

    void update_max_price(int new_price) {
        int current_max = _max_price.load();

        // --- TODO: 你的无锁闭环逻辑 ---
        // 1. 写一个 while 循环。只要 new_price 大于 current_max，就尝试去更新它。
        // 2. 在循环里调用 _max_price.compare_exchange_weak(current_max, new_price)。
        // 3. 如果 CAS 成功，说明价格更新完成，直接退出循环（或者 return）。
        // 4. 如果 CAS 失败，current_max 会被硬件自动刷新为最新的全局最大值，继续下一次循环判断。

        
    }

    int get_max_price() { return _max_price.load(); }

private:
    std::atomic<int> _max_price;
};