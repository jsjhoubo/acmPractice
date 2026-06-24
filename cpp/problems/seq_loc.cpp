#include <atomic>
#include <immintrin.h>

struct AtomicMarketData {
    std::atomic<double> bid_price;
    std::atomic<double> ask_price;
    std::atomic<uint64_t> volume;
};
struct MarketData {
    double bid_price;
    double ask_price;
    uint64_t volume;
};

class alignas(64) SeqLockData {
private:
    std::atomic<uint64_t> _version{0}; // 偶数表示空闲，奇数表示正在写入
    AtomicMarketData _data;

public:
    // 写者（只有一个线程执行）
    void write(const MarketData& new_data) {
        uint64_t v = _version.load(std::memory_order_relaxed);
        _version.store(v + 1, std::memory_order_relaxed); // 变为奇数，加锁


        std::atomic_thread_fence(std::memory_order_release);
        _data.bid_price.store(new_data.bid_price, std::memory_order_relaxed); 
        _data.ask_price.store(new_data.ask_price, std::memory_order_relaxed); 
        _data.volume.store(new_data.volume, std::memory_order_relaxed); 
        _version.store(v + 2, std::memory_order_release); // 变为偶数，解锁
    }

    // 读者（成百上千个线程并发执行）
    MarketData read() {
        MarketData copy;
        uint64_t v1, v2;
        do {
            v1 = _version.load(std::memory_order_acquire);
            
            // 如果是奇数，说明写者正在写，原地自旋等待
            if (v1 % 2 != 0) {
                _mm_pause();
                
                continue;
            }

            copy.ask_price = _data.ask_price.load(std::memory_order_relaxed);
            copy.bid_price = _data.bid_price.load(std::memory_order_relaxed);
            copy.volume = _data.volume.load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acquire);
            v2 = _version.load(std::memory_order_relaxed);
            // 如果 v1 == v2，说明拷贝期间没被别人动过，安全！
        } while (v1 != v2 || v1 & 1);

        return copy;
    }
};