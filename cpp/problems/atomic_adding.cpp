#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <cassert>

std::atomic<int> g_sum(0);

void worker(int n) {
    for (int i = 0; i < n; ++i) {
        // --- TODO: 你的核心代码 (一�? ---
        g_sum ++;
    }
}

int main() {
    int iters = 100000;
    std::thread t1(worker, iters);
    std::thread t2(worker, iters);
    t1.join(); t2.join();

    std::cout << "Final Sum: " << g_sum << std::endl;
    assert(g_sum == 200000);
    return 0;
}
