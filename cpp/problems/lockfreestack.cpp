#include <atomic>

struct Node {
    int value;
    Node* next;
};

class LockFreeStack {
    std::atomic<Node*> _head{nullptr};

public:
    void push(Node* new_node) {
        new_node->next = _head.load(std::memory_order_relaxed);
        while (!_head.compare_exchange_weak(new_node->next, new_node, 
                                            std::memory_order_release, 
                                            std::memory_order_relaxed));
    }

    Node* pop() {
        Node* old_head = _head.load(std::memory_order_acquire);
        do {
            if (!old_head) return nullptr;
            // 试图把 head 从 old_head 指向 old_head->next
        } while (!_head.compare_exchange_weak(old_head, old_head->next, 
                                              std::memory_order_acquire, 
                                              std::memory_order_acquire));
        return old_head;
    }
};