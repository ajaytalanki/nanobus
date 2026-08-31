#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <string>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>

// Message Frame Interface
// [1 Byte Command] [2 Bytes Topic Length] [N Bytes Topic] [4 Bytes Payload Length] [M Bytes Payload]

enum Command : uint8_t {
    cmdSub   = 0x01,
    cmdUnsub = 0x02,
    cmdPub   = 0x03
};

struct MessageFrame {
    std::string topic;
    std::vector<uint8_t> payload;
};

struct Connection {
    int fd;
    std::vector<uint8_t> readBuf;
    std::vector<uint8_t> writeBuf;

    void setNonblocking() {
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
};

class Router {
private:
    std::unordered_map<std::string, std::unordered_set<int>> subscriptions;
    std::unordered_map<int, std::unordered_set<std::string>> clientTopics;

public:
    void subscribe(int fd, const std::string& prefix) {
        subscriptions[prefix].insert(fd);
        clientTopics[fd].insert(prefix);
    }

    void unsubscribe(int fd, const std::string& prefix) {
        if (auto it = subscriptions.find(prefix); it != subscriptions.end()) {
            it->second.erase(fd);
            if (it->second.empty()) subscriptions.erase(it);
        }
        if (auto it = clientTopics.find(fd); it != clientTopics.end()) {
            it->second.erase(prefix);
            if (it->second.empty()) clientTopics.erase(it);
        }
    }

    void removeClient(int fd) {
        if (auto it = clientTopics.find(fd); it != clientTopics.end()) {
            for (const auto& prefix : it->second) {
                if (auto subIt = subscriptions.find(prefix); subIt != subscriptions.end()) {
                    subIt->second.erase(fd);
                    if (subIt->second.empty()) subscriptions.erase(subIt);
                }
            }
            clientTopics.erase(it);
        }
    }

    std::vector<int> getMatchedSubscribers(const std::string& topic) const {
        std::vector<int> matchedFds;
        for (const auto& [prefix, clients] : subscriptions) {
            if (topic.rfind(prefix, 0) == 0) { // Prefix match check
                for (int fd : clients) {
                    matchedFds.push_back(fd);
                }
            }
        }
        std::sort(matchedFds.begin(), matchedFds.end());
        matchedFds.erase(std::unique(matchedFds.begin(), matchedFds.end()), matchedFds.end());
        return matchedFds;
    }
};

class Broker {
private:
    int listenFd;
    int epollFd;
    Router router;
    std::unordered_map<int, std::unique_ptr<Connection>> connections;

public:
    Broker(int port) {
        listenFd = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        bind(listenFd, (struct sockaddr*)&addr, sizeof(addr));
        listen(listenFd, SOMAXCONN);

        int flags = fcntl(listenFd, F_GETFL, 0);
        fcntl(listenFd, F_SETFL, flags | O_NONBLOCK);

        epollFd = epoll_create1(0);
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = listenFd;
        epoll_ctl(epollFd, EPOLL_CTL_ADD, listenFd, &ev);
    }

    ~Broker() {
        close(listenFd);
        close(epollFd);
    }

    void run() {
        constexpr int maxEvents = 64;
        epoll_event events[maxEvents];

        std::cout << "Broker running on epoll event loop...\n";

        while (true) {
            int nfds = epoll_wait(epollFd, events, maxEvents, -1);
            for (int i = 0; i < nfds; ++i) {
                int fd = events[i].data.fd;

                if (fd == listenFd) {
                    acceptConnections();
                } else {
                    if (events[i].events & EPOLLIN) {
                        handleRead(fd);
                    }
                    if (events[i].events & EPOLLOUT) {
                        handleWrite(fd);
                    }
                }
            }
        }
    }

private:
    void acceptConnections() {
        while (true) {
            sockaddr_in clientAddr;
            socklen_t clientLen = sizeof(clientAddr);
            int clientFd = accept(listenFd, (struct sockaddr*)&clientAddr, &clientLen);
            
            if (clientFd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break; // Drained all pending connections
                break;
            }

            auto conn = std::make_unique<Connection>(Connection{clientFd});
            conn->setNonblocking();

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
            ev.data.fd = clientFd;
            epoll_ctl(epollFd, EPOLL_CTL_ADD, clientFd, &ev);

            connections[clientFd] = std::move(conn);
        }
    }

    void handleRead(int fd) {
        auto connIt = connections.find(fd);
        if (connIt == connections.end()) return;
        auto& conn = connIt->second;

        uint8_t buffer[4096];
        while (true) {
            ssize_t bytesRead = read(fd, buffer, sizeof(buffer));
            if (bytesRead > 0) {
                conn->readBuf.insert(conn->readBuf.end(), buffer, buffer + bytesRead);
            } else if (bytesRead == 0 || (bytesRead < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                disconnectClient(fd);
                return;
            } else {
                break; // Socket buffer fully drained (EAGAIN)
            }
        }

        processFrames(*conn);
    }

    void processFrames(Connection& conn) {
        while (true) {
            // Header length check: 1 byte (cmd) + 2 bytes (topicLen) = 3 bytes minimum
            if (conn.readBuf.size() < 3) return;

            uint8_t cmd = conn.readBuf[0];
            uint16_t topicLen = (conn.readBuf[1] << 8) | conn.readBuf[2];

            size_t minFrameLen = 3 + topicLen;
            if (cmd == cmdPub) {
                minFrameLen += 4; // 4 bytes for payload length
            }

            if (conn.readBuf.size() < minFrameLen) return; // Incomplete header frame

            uint32_t payloadLen = 0;
            if (cmd == cmdPub) {
                payloadLen = (conn.readBuf[3 + topicLen] << 24) |
                             (conn.readBuf[3 + topicLen + 1] << 16) |
                             (conn.readBuf[3 + topicLen + 2] << 8) |
                              conn.readBuf[3 + topicLen + 3];
            }

            size_t totalFrameLen = minFrameLen + payloadLen;
            if (conn.readBuf.size() < totalFrameLen) return; // Wait for full frame body

            // Extract Frame Data
            std::string topic(conn.readBuf.begin() + 3, conn.readBuf.begin() + 3 + topicLen);

            if (cmd == cmdSub) {
                router.subscribe(conn.fd, topic);
            } else if (cmd == cmdUnsub) {
                router.unsubscribe(conn.fd, topic);
            } else if (cmd == cmdPub) {
                auto frame = std::make_shared<MessageFrame>();
                frame->topic = topic;
                frame->payload.assign(
                    conn.readBuf.begin() + minFrameLen,
                    conn.readBuf.begin() + totalFrameLen
                );
                fanoutMessage(frame);
            }

            // Consume frame bytes from read buffer
            conn.readBuf.erase(conn.readBuf.begin(), conn.readBuf.begin() + totalFrameLen);
        }
    }

    void fanoutMessage(const std::shared_ptr<MessageFrame>& frame) {
        auto targetFds = router.getMatchedSubscribers(frame->topic);

        // Serialize frame payload once for all outgoing matching sockets
        std::vector<uint8_t> serialized;
        uint16_t topicLen = htons(frame->topic.size());
        uint32_t payloadLen = htonl(frame->payload.size());

        serialized.push_back(cmdPub);
        serialized.insert(serialized.end(), (uint8_t*)&topicLen, (uint8_t*)&topicLen + 2);
        serialized.insert(serialized.end(), frame->topic.begin(), frame->topic.end());
        serialized.insert(serialized.end(), (uint8_t*)&payloadLen, (uint8_t*)&payloadLen + 4);
        serialized.insert(serialized.end(), frame->payload.begin(), frame->payload.end());

        for (int subFd : targetFds) {
            if (auto it = connections.find(subFd); it != connections.end()) {
                it->second->writeBuf.insert(it->second->writeBuf.end(), serialized.begin(), serialized.end());
                handleWrite(subFd);
            }
        }
    }

    void handleWrite(int fd) {
        auto it = connections.find(fd);
        if (it == connections.end()) return;
        auto& conn = it->second;

        while (!conn->writeBuf.empty()) {
            ssize_t bytesWritten = write(fd, conn->writeBuf.data(), conn->writeBuf.size());
            if (bytesWritten > 0) {
                conn->writeBuf.erase(conn->writeBuf.begin(), conn->writeBuf.begin() + bytesWritten);
            } else if (bytesWritten < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break; // Socket send buffer full, keep remaining data in writeBuf
            } else {
                disconnectClient(fd);
                return;
            }
        }
    }

    void disconnectClient(int fd) {
        epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        router.removeClient(fd);
        connections.erase(fd);
    }
};

int main() {
    Broker broker(8080);
    broker.run();
    return 0;
}