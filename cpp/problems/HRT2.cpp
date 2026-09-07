#include <iostream>
#include <vector>
#include <algorithm>
#include <cassert>

using namespace std;

int countPlayersReached(
    int L,
    vector<int> players,
    int watcherStart,
    vector<int> flipTimes,
    int T
) {
    // TODO: implement here
    sort(flipTimes.begin(), flipTimes.end());
    int dir =-1;
    int j =0;
    int watcher_location =watcherStart;
    for (int i =0;i<T;i++) {
        if (j < flipTimes.size() && i == flipTimes[j]) {
            dir = - dir;
            j++;
        }
        for (int k =0;k<players.size();k++) {
            if (players[k] <L && dir ==1 && players[k] < watcher_location) {
                players[k] +=1;
            }
            else if (players[k] <L && dir ==-1 && players[k] > watcher_location) {
                players[k] +=1;
            }
        }
        watcher_location += dir;
    }
    int cnt =0;
    for (int i=0;i<players.size();i++) {
        if (players[i] ==L) {
            cnt ++;
        }
    }
    return cnt;
}

int main() {
    // Test 1: example
    assert(countPlayersReached(
        5,
        {1, 4},
        3,
        {1},
        2
    ) == 1);

    // Test 2: no flips
    assert(countPlayersReached(
        10,
        {2, 8},
        5,
        {},
        3
    ) == 1);

    // Test 3: unordered flip times
    assert(countPlayersReached(
        10,
        {1, 6, 8},
        5,
        {3, 1},
        4
    ) == 1);

    // Test 4: player initially at goal
    assert(countPlayersReached(
        5,
        {5, 4, 0},
        2,
        {1},
        2
    ) == 2);

    // Test 5: multiple flips
    assert(countPlayersReached(
        8,
        {1, 3, 6},
        4,
        {2, 1, 3},
        4
    ) == 1);

    // Test 6: T = 0
    assert(countPlayersReached(
        5,
        {5, 4, 2},
        3,
        {0},
        0
    ) == 1);

    cout << "All tests passed!" << endl;

    return 0;
}