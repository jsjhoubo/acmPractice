#include <iostream>
#include <atomic>
#include <chrono>

class LockFreeRateLimiter {
public:
    LockFreeRateLimiter(int limit) : _limit(limit), _count(0) {
        _last_reset_second = get_current_second();
    }

    bool is_allowed() {
        int64_t now_sec = get_current_second();
        int64_t current_reset_sec = _last_reset_second.load();

        if (now_sec > current_reset_sec) {
            // --- TODO 1: 尝试用 CAS 更新时间戳 ---
            // 提示：抢着把 _last_reset_second 从 current_reset_sec 变成 now_sec
            if (_last_reset_second.compare_exchange_strong(current_reset_sec, now_sec)) {
                // 只有一个线程能抢成功！
                // --- TODO 2: 重置计数器 ---
                int space = now_sec - 
                
            }
        }

        // --- TODO 3: 原子加1并判断 ---
        // 利用 fetch_add 确保获取安全的当前快照值
        return false; // 你的实现
    }

private:
    int64_t get_current_second() {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    }

    int _limit;
    std::atomic<int> _count;
    std::atomic<int64_t> _last_reset_second; // 用原子变量存秒
};