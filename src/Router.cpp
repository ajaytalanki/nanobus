#include "nanobus/Router.hpp"

#include <algorithm>

namespace nanobus {

void Router::subscribe(int fd, const std::string& prefix) {
    subscriptions_[prefix].insert(fd);
    clientTopics_[fd].insert(prefix);
}

void Router::unsubscribe(int fd, const std::string& prefix) {
    if (auto it = subscriptions_.find(prefix); it != subscriptions_.end()) {
        it->second.erase(fd);
        if (it->second.empty()) {
            subscriptions_.erase(it);
        }
    }

    if (auto it = clientTopics_.find(fd); it != clientTopics_.end()) {
        it->second.erase(prefix);
        if (it->second.empty()) {
            clientTopics_.erase(it);
        }
    }
}

void Router::removeClient(int fd) {
    if (auto it = clientTopics_.find(fd); it != clientTopics_.end()) {
        for (const auto& prefix : it->second) {
            if (auto subIt = subscriptions_.find(prefix); subIt != subscriptions_.end()) {
                subIt->second.erase(fd);
                if (subIt->second.empty()) {
                    subscriptions_.erase(subIt);
                }
            }
        }
        clientTopics_.erase(it);
    }
}

std::vector<int> Router::getMatchedSubscribers(const std::string& topic) const {
    std::vector<int> matchedFds;
    for (const auto& [prefix, clients] : subscriptions_) {
        if (topic == prefix) {
            for (int fd : clients) {
                matchedFds.push_back(fd);
            }
        }
    }

    std::sort(matchedFds.begin(), matchedFds.end());
    matchedFds.erase(std::unique(matchedFds.begin(), matchedFds.end()), matchedFds.end());
    return matchedFds;
}

} // namespace nanobus
