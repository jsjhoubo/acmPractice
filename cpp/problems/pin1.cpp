#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <utility>
#include <algorithm> 

using namespace std;

// Each save record: (user_id, pin_id, board_id)
struct Save {
    string user_id;
    string pin_id;
    string board_id;
};

// Return up to k pins that co-occur with target_pin in the most distinct boards.
// Co-occurrence = target_pin and another pin appear in the same board.
// A pin saved multiple times to the same board counts once.
// target_pin itself is excluded from the result.
vector<string> findSimilarPins(const vector<Save>& saves,
                               const string& target_pin,
                               int k) {
    // 1) build maps
    unordered_set<string> boards;   // pin   -> set(board)
    unordered_map<string, unordered_set<string>> pin_to_boards;   
    // TODO
    for (auto &save: saves) {
        if (save.pin_id == target_pin) {
            boards.insert(save.board_id);
        }
    }
    for (auto &save : saves) {
        if (boards.find(save.board_id)!=boards.end()) {
            if (save.pin_id != target_pin) {
                pin_to_boards[save.pin_id].insert(save.board_id);
            }
        }
    }

    priority_queue<pair<int, string>, vector<pair<int, string>>, std::greater<pair<int, string>>> pq;

    for (auto & [pin, bs] : pin_to_boards) {
        if (pq.size() < k) {
            pq.push({bs.size(), pin});
        }
        else {
            if (bs.size() > pq.top().first) {
                pq.pop();
                pq.push({bs.size(), pin});
            }
        }
    }
    vector<string> result;
    while (!pq.empty()) {
        auto &[cnt, board_id]  =pq.top();
        result.push_back(board_id);
        pq.pop();
    }
    reverse(result.begin(), result.end());
    return result;
}