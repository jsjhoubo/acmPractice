#include <atomic>
#include <thread>
#include <immintrin.h>

class Spinlock {
public:
    void lock() {
        bool expected = false;

        // --- TODO 1: 核心锁死逻辑 ---
        // 你的任务：写一个循环。
        // 尝试通过 CAS 把 _locked 从 false（expected） 改成 true。
        // 如果抢锁失败，expected 会被硬件自动刷成 true，
        // 你必须在下一圈循环前，把它重新设为 false，然后继续抢！
        while (!_locked.compare_exchange_weak(expected, true)) {
            expected = false; // 抢输了，重置期望值，继续发起下一次原子冲锋
            
            // --- TODO 2: HFT 终极性能优化 ---
            // 提示：如果在这里什么都不写，CPU 的一个核心会以 100% 的功耗疯狂空转，
            // 导致 CPU 极度发热，并强占总线带宽，拖慢真正持有锁的那个线程。
            // 业界标准的做法是在这里加入一条“硬件暗示指令”，怎么写？
#ifdef _MSC_VER
    #include <intrin.h>
    #define PAUSE() _mm_pause()
#elif defined(__GNUC__) || defined(__clang__)
    #include <x86intrin.h>
    #define PAUSE() _mm_pause()
#else
    #define PAUSE() __asm__ volatile ("rep; nop" ::: "memory")
#endif
        }
    }

    void unlock() {
        // 原子地释放锁
        _locked.store(false);
    }

private:
    std::atomic<bool> _locked{false};
};