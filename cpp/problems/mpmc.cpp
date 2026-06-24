#include <atomic>
#include <cstddef>
#include <cstdint>

template <typename T, size_t N>   // N = 2 的幂
class MpmcQueue {
    static_assert((N & (N - 1)) == 0, "N must be power of 2");

    struct Slot {
        std::atomic<uint64_t> token;   // 当前可操作此 slot 的 token(P+1 写完可读 / P 可写)
        T data;
    };

    alignas(64) Slot slots_[N];
    alignas(64) std::atomic<uint64_t> tail_{0};   // 写线程抢:enqueue token
    alignas(64) std::atomic<uint64_t> head_{0};   // 读线程抢:dequeue token

public:
    MpmcQueue() {
        for (size_t i = 0; i < N; ++i)
            slots_[i].token.store(i, std::memory_order_relaxed);
    }

    bool enqueue(const T& item) {
        // 你来填
        uint64_t h = head_.load(std::memory_order_relaxed);
        uint64_t t = tail_.load(std::memory_order_acquired);
        while (t -h <N && !tail_.compare_exchange_weak(t, t+1, std::memory_order_release, std::memory_order_relaxed)) {

        }
        if (t-h == N) {
            return false;
        }
        uint64_t loc =t & (N-1);
        uint64_t token =slots_[loc].token.load(std::memory_order_acquired);
        
        while (token !=t) {
            _mm_puase();
            token =slots_[loc].token.load(std::memory_order_acquired);
        }
        slots_[loc].data =item;
        slots_[loc].token.store(t+1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& out) {
        // 你来填
        uint64_t t = tail_.load(std::memory_order_relaxed);
        uint64_t h = head_.load(std::memory_order_acquired);
        while (h < t && !head_.compare_exchange_weak(h, h+1, std::memory_order_release, std::memory_order_relaxed)) {

        }
        if (t == h) {
            return false;
        }
        uint64_t loc =h & (N-1);
        uint64_t token =slots_[loc].token.load(std::memory_order_acquired);
        
        while (token !=h+1) {
            _mm_puase();
            token =slots_[loc].token.load(std::memory_order_acquired);
        }
        out = slots_[loc].data;
        slots_[loc].token.store(h+N, std::memory_order_release);
        return true;
    }
};