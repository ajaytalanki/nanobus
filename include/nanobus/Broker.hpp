#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "nanobus/Connection.hpp"
#include "nanobus/MessageFrame.hpp"
#include "nanobus/Protocol.hpp"
#include "nanobus/Router.hpp"

namespace nanobus {

class Broker {
public:
    explicit Broker(int port);
    ~Broker();

    void run();

private:
    void acceptConnections();
    void handleRead(int fd);
    void processFrames(Connection& conn);
    void fanoutMessage(const std::shared_ptr<MessageFrame>& frame);
    void handleWrite(int fd);
    void disconnectClient(int fd);

    int listenFd_;
    int epollFd_;
    Router router_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
};

} // namespace nanobus
