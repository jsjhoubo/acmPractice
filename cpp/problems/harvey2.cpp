#include <map>
#include <algorithm>
#include <vector>

using namespace std;

class HighlightOperation
{
    map<int, int> _cache;

public:
    void add_highlight(int start, int end)
    {
        auto iter = _cache.lower_bound(start);

        if (iter != _cache.begin())
        {
            --iter;
            if (iter->second >= start)
            {
                start = iter->first;
            }
            else
            {
                ++iter;
            }
        }
        while (iter != _cache.end())
        {
            if (iter->first > end)
            {
                break;
            }
            end = max(iter->second, end);
            iter = _cache.erase(iter);
        }
        _cache[start] = end;
    }

    vector<pair<int, int>> get_highlights()
    {
        vector<pair<int, int>> ret;
        for (auto &[s, e] : _cache)
        {
            ret.push_back({s, e});
        }
        return ret;
    }
};

int main()
{
    return 0;
}