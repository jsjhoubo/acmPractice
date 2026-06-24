#include <iostream>
#include <atomic>
#include <thread>

std::atomic<int> g_state(0); // 0: Ready, 1: Running

void try_start(int thread_id) {
    int expected = 0;
    int desired = 1;

    // --- TODO: 请在这里填入你的核心代码 ---
    // 使用 g_state.compare_exchange_strong
    if (g_state.compare_exchange_strong(expected, desired)) {
        std::cout << "Thread " << thread_id << " set state to RUNNING!" << std::endl;
    } else {
        std::cout << "Thread " << thread_id << " failed (already running)." << std::endl;
    }
}

int main() {
    std::thread t1(try_start, 1);
    std::thread t2(try_start, 2);
    t1.join(); t2.join();
    return 0;
}