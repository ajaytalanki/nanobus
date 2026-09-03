#include "nanobus/Broker.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace nanobus {

Broker::Broker(int port) {
    listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
        throw std::runtime_error("Failed to create listening socket");
    }

    int opt = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listenFd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error("Failed to bind broker socket");
    }

    if (listen(listenFd_, SOMAXCONN) < 0) {
        throw std::runtime_error("Failed to listen on broker socket");
    }

    int flags = fcntl(listenFd_, F_GETFL, 0);
    fcntl(listenFd_, F_SETFL, flags | O_NONBLOCK);

    epollFd_ = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listenFd_;
    epoll_ctl(epollFd_, EPOLL_CTL_ADD, listenFd_, &ev);
}

Broker::~Broker() {
    close(listenFd_);
    close(epollFd_);
}

void Broker::run() {
    constexpr int maxEvents = 64;
    epoll_event events[maxEvents];

    std::cout << "Broker running on epoll event loop...\n";

    while (true) {
        int nfds = epoll_wait(epollFd_, events, maxEvents, -1);
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            if (fd == listenFd_) {
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

void Broker::acceptConnections() {
    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = accept(listenFd_, reinterpret_cast<struct sockaddr*>(&clientAddr), &clientLen);

        if (clientFd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            break;
        }

        auto conn = std::make_unique<Connection>(Connection{clientFd});
        conn->setNonblocking();

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = clientFd;
        epoll_ctl(epollFd_, EPOLL_CTL_ADD, clientFd, &ev);

        connections_[clientFd] = std::move(conn);
    }
}

void Broker::handleRead(int fd) {
    auto connIt = connections_.find(fd);
    if (connIt == connections_.end()) {
        return;
    }
    auto& conn = connIt->second;
    bool shouldDisconnect = false;

    uint8_t buffer[4096];
    while (true) {
        ssize_t bytesRead = read(fd, buffer, sizeof(buffer));
        if (bytesRead > 0) {
            conn->readBuf.insert(conn->readBuf.end(), buffer, buffer + bytesRead);
        } else if (bytesRead == 0 || (bytesRead < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
            shouldDisconnect = true;
            break;
        } else {
            break;
        }
    }

    processFrames(*conn);
    if (shouldDisconnect && connections_.contains(fd)) {
        disconnectClient(fd);
    }
}

void Broker::processFrames(Connection& conn) {
    while (true) {
        if (conn.readBuf.size() < 3) {
            return;
        }

        uint8_t cmd = conn.readBuf[0];
        uint16_t topicLen = (static_cast<uint16_t>(conn.readBuf[1]) << 8) |
                            static_cast<uint16_t>(conn.readBuf[2]);

        size_t minFrameLen = 3 + topicLen;
        if (cmd == static_cast<uint8_t>(Command::Pub)) {
            minFrameLen += 4;
        }

        if (conn.readBuf.size() < minFrameLen) {
            return;
        }

        uint32_t payloadLen = 0;
        if (cmd == static_cast<uint8_t>(Command::Pub)) {
            payloadLen = (static_cast<uint32_t>(conn.readBuf[3 + topicLen]) << 24) |
                         (static_cast<uint32_t>(conn.readBuf[3 + topicLen + 1]) << 16) |
                         (static_cast<uint32_t>(conn.readBuf[3 + topicLen + 2]) << 8) |
                          static_cast<uint32_t>(conn.readBuf[3 + topicLen + 3]);
        }

        size_t totalFrameLen = minFrameLen + payloadLen;
        if (conn.readBuf.size() < totalFrameLen) {
            return;
        }

        std::string topic(conn.readBuf.begin() + 3, conn.readBuf.begin() + 3 + topicLen);

        if (cmd == static_cast<uint8_t>(Command::Sub)) {
            router_.subscribe(conn.fd, topic);
        } else if (cmd == static_cast<uint8_t>(Command::Unsub)) {
            router_.unsubscribe(conn.fd, topic);
        } else if (cmd == static_cast<uint8_t>(Command::Pub)) {
            auto frame = std::make_shared<MessageFrame>();
            frame->topic = topic;
            frame->payload.assign(conn.readBuf.begin() + minFrameLen,
                                  conn.readBuf.begin() + totalFrameLen);
            fanoutMessage(frame);
        }

        conn.readBuf.erase(conn.readBuf.begin(), conn.readBuf.begin() + totalFrameLen);
    }
}

void Broker::fanoutMessage(const std::shared_ptr<MessageFrame>& frame) {
    auto targetFds = router_.getMatchedSubscribers(frame->topic);

    std::vector<uint8_t> serialized;
    uint16_t topicLen = htons(static_cast<uint16_t>(frame->topic.size()));
    uint32_t payloadLen = htonl(static_cast<uint32_t>(frame->payload.size()));

    serialized.push_back(static_cast<uint8_t>(Command::Pub));
    serialized.insert(serialized.end(), reinterpret_cast<uint8_t*>(&topicLen), reinterpret_cast<uint8_t*>(&topicLen) + 2);
    serialized.insert(serialized.end(), frame->topic.begin(), frame->topic.end());
    serialized.insert(serialized.end(), reinterpret_cast<uint8_t*>(&payloadLen), reinterpret_cast<uint8_t*>(&payloadLen) + 4);
    serialized.insert(serialized.end(), frame->payload.begin(), frame->payload.end());

    for (int subFd : targetFds) {
        if (auto it = connections_.find(subFd); it != connections_.end()) {
            it->second->writeBuf.insert(it->second->writeBuf.end(), serialized.begin(), serialized.end());
            handleWrite(subFd);
        }
    }
}

void Broker::handleWrite(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }

    auto& conn = it->second;
    while (!conn->writeBuf.empty()) {
        ssize_t bytesWritten = write(fd, conn->writeBuf.data(), conn->writeBuf.size());
        if (bytesWritten > 0) {
            conn->writeBuf.erase(conn->writeBuf.begin(), conn->writeBuf.begin() + bytesWritten);
        } else if (bytesWritten < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            disconnectClient(fd);
            return;
        }
    }
}

void Broker::disconnectClient(int fd) {
    epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    router_.removeClient(fd);
    connections_.erase(fd);
}

} // namespace nanobus
