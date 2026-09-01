#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "nanobus/Protocol.hpp"

namespace nanobus {

class Client {
protected:
    int socketFd_ = -1;

    bool connectToBroker(const std::string& host, int port);
    void sendFrame(Command cmd, const std::string& topic, const std::string& payload = "");

public:
    virtual ~Client();
};

class Publisher : public Client {
public:
    bool connect(const std::string& host = "127.0.0.1", int port = 8080);
    void publish(const std::string& topic, const std::string& message);
};

class Subscriber : public Client {
private:
    std::thread workerThread_;
    std::atomic<bool> running_{false};

public:
    bool connect(const std::string& host = "127.0.0.1", int port = 8080);
    void subscribe(const std::string& topicPrefix);
    void unsubscribe(const std::string& topicPrefix);
    void listen(std::function<void(const std::string&, const std::string&)> onMessage);
    void stop();
    ~Subscriber();
};

} // namespace nanobus
