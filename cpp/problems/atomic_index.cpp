#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <cassert>

std::atomic<int> g_task_index(0);
const int TOTAL_TASKS = 100;
int g_processed_count[TOTAL_TASKS] = {0}; // 用于校验

void fetch_and_process() {
    while (true) {
        // --- TODO: 你的核心代码 ---
        // 1. 原子地获取当前下标，并将下标加 1 (使用 fetch_add)
        // 2. 判断下标是否超过 TOTAL_TASKS，超过则 break
        
        int idx = g_task_index ++;
        
        if (idx >= TOTAL_TASKS) break;
        
        // 模拟处理任务
        g_processed_count[idx]++; 
    }
}

int main() {
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) threads.emplace_back(fetch_and_process);
    for (auto& t : threads) t.join();

    // 校验：每个任务是否都被处理过且仅处理过一次
    for (int i = 0; i < TOTAL_TASKS; ++i) {
        assert(g_processed_count[i] == 1);
    }
    std::cout << "Task Indexer Test: PASSED" << std::endl;
    return 0;
}