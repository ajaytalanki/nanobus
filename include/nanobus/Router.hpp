#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace nanobus {

class Router {
public:
    void subscribe(int fd, const std::string& prefix);
    void unsubscribe(int fd, const std::string& prefix);
    void removeClient(int fd);
    std::vector<int> getMatchedSubscribers(const std::string& topic) const;

private:
    std::unordered_map<std::string, std::unordered_set<int>> subscriptions_;
    std::unordered_map<int, std::unordered_set<std::string>> clientTopics_;
};

} // namespace nanobus
