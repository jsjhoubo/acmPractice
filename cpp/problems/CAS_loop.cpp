#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <algorithm>

std::atomic<int> g_max_value(0);

void update_max(int val) {
    int current_max = g_max_value.load();
    
    // --- TODO: 你的核心代码 (CAS 循环) ---
    // 逻辑：
    // 1. 如果新值 val 比 current_max 小，直接返回。
    // 2. 如果新值更大，尝试用 CAS 将 g_max_value 从 current_max 修改为 val。
    // 3. 如果修改失败（说明别的线程改了最大值），更新 current_max 并重试。
    if (val < current_max) {
        return;
    }
    
    while (current_max < val && 
           !g_max_value.compare_exchange_strong(current_max, val)) {
        // 循环体为空，compare_exchange_strong 失败时会自动更新 current_max
    }
}

int main() {
    std::vector<int> data = {10, 50, 20, 100, 30, 80};
    std::vector<std::thread> threads;
    
    for (int v : data) {
        threads.emplace_back(update_max, v);
    }
    
    for (auto& t : threads) t.join();
    
    std::cout << "Final Max Value: " << g_max_value.load() << std::endl;
    // 应输出 100
    return 0;
}