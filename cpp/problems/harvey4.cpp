#include <vector>
#include <unordered_set>
#include <algorithm>
#include <string>
#include <set>
#include <iostream>

using namespace std;
struct Result {
    string sentence;
    set<int> valid_citations;
    set<int> invalid_citations;
};

class ProcessText {
    static string trim_range(const string & text) {
        const auto first = text.find_first_not_of(" \t\n\r\f\v");
        if (first == string::npos) {
                return {};
        }

        const auto last = text.find_last_not_of(" \t\n\r\f\v");
        return text.substr(first, last - first + 1);
    }
public:
  vector<Result> Process(const vector<string> & sources, const string & answer) {
    vector<Result> results;
    int i= 0;
    string sentence;
    set<int> valid_citations;
    set<int> invalid_citations;
    while(i< answer.size()) {
        if (answer[i] =='.') {
            results.push_back(Result{trim_range(sentence), valid_citations, invalid_citations});
            sentence ="";
            valid_citations.clear();
            invalid_citations.clear();
        }
        else if (answer[i]=='[') {
            i++;
            if (!(i< answer.size() && answer[i] >='0' && answer[i] <='9')) {
                sentence += '[';
                continue;
            }
            int citation =0;
            
            bool has_number =false;
            int j =i;
            while (i< answer.size() && answer[i] >='0' && answer[i] <='9') {
                citation = citation * 10 + answer[i] -'0';
                i++;
                has_number =true;
            }
            if (has_number && i< answer.size() && answer[i]==']') {
                
                    if (citation < sources.size()) {
                        valid_citations.insert(citation);
                    }
                    else {
                        invalid_citations.insert(citation);
                    }
                
            }
            else {
                sentence += "[" + answer.substr(j, i -j);
                continue;
            }
        }
        else {
            sentence += answer[i]; 
        }
        i++;
    }
    if (sentence.size() >0) {
        results.push_back({sentence, valid_citations, invalid_citations});
    }
    return results;
  }  
};

int main() {
    vector<string> sources = {
        "Rent is due monthly.",
        "Either party may terminate with 30 days notice."
    };

    string answer =
        "The tenant must pay rent every month [0]. The lease can be "
        "ended with a month's notice [1]. Late fees apply after five days.";

    ProcessText processor;
    auto results = processor.Process(sources, answer);

    for (const auto & result : results) {
        cout << "text: " << result.sentence << '\n';
        cout << "valid citations: ";
        for (int citation : result.valid_citations) {
            cout << citation << ' ';
        }
        cout << '\n';
        cout << "invalid citations: ";
        for (int citation : result.invalid_citations) {
            cout << citation << ' ';
        }
        cout << '\n';
        cout << "---\n";
    }

    return 0;
}