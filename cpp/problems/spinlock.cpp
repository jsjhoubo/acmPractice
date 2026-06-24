#include <iostream>
#include <atomic>
#include <thread>
#include <vector>

class SimpleSpinlock {
    // ATOMIC_FLAG_INIT 初始化为 clear 状态
    std::atomic_flag flag = ATOMIC_FLAG_INIT; 
public:
    void lock() {
        // --- TODO: 你的核心代码 ---
        // 提示：只要 test_and_set() 返回 true，说明别人已经占了锁，你就得继续循环
        while (flag.test_and_set(std::memory_order_acquire) == true);
    }

    void unlock() {
        // --- TODO: 你的核心代码 ---
        // 提示：将 flag 清除
        flag.clear(std::memory_order_release);
    }
};

int g_shared_data = 0;
SimpleSpinlock g_spin;

void safe_inc() {
    for(int i=0; i<10000; ++i) {
        g_spin.lock();
        g_shared_data++;
        g_spin.unlock();
    }
}

int main() {
    std::thread t1(safe_inc);
    std::thread t2(safe_inc);
    t1.join(); t2.join();
    std::cout << "Final Shared Data: " << g_shared_data << std::endl; // 应该是 20000
    return 0;
}