#include "nanobus/Client.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace nanobus {

bool Client::connectToBroker(const std::string& host, int port) {
    socketFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd_ < 0) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    return connect(socketFd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0;
}

void Client::sendFrame(Command cmd, const std::string& topic, const std::string& payload) {
    std::vector<uint8_t> frame;
    uint16_t topicLen = htons(static_cast<uint16_t>(topic.size()));

    frame.push_back(static_cast<uint8_t>(cmd));
    frame.insert(frame.end(), reinterpret_cast<uint8_t*>(&topicLen), reinterpret_cast<uint8_t*>(&topicLen) + 2);
    frame.insert(frame.end(), topic.begin(), topic.end());

    if (cmd == Command::Pub) {
        uint32_t payloadLen = htonl(static_cast<uint32_t>(payload.size()));
        frame.insert(frame.end(), reinterpret_cast<uint8_t*>(&payloadLen), reinterpret_cast<uint8_t*>(&payloadLen) + 4);
        frame.insert(frame.end(), payload.begin(), payload.end());
    }

    send(socketFd_, frame.data(), frame.size(), 0);
}

Client::~Client() {
    if (socketFd_ >= 0) {
        close(socketFd_);
        socketFd_ = -1;
    }
}

bool Publisher::connect(const std::string& host, int port) {
    return connectToBroker(host, port);
}

void Publisher::publish(const std::string& topic, const std::string& message) {
    sendFrame(Command::Pub, topic, message);
}

bool Subscriber::connect(const std::string& host, int port) {
    return connectToBroker(host, port);
}

void Subscriber::subscribe(const std::string& topicPrefix) {
    sendFrame(Command::Sub, topicPrefix);
}

void Subscriber::unsubscribe(const std::string& topicPrefix) {
    sendFrame(Command::Unsub, topicPrefix);
}

void Subscriber::listen(std::function<void(const std::string&, const std::string&)> onMessage) {
    running_ = true;
    workerThread_ = std::thread([this, onMessage]() {
        std::vector<uint8_t> buffer;
        uint8_t tempBuf[4096];

        while (running_) {
            ssize_t bytesRead = recv(socketFd_, tempBuf, sizeof(tempBuf), 0);
            if (bytesRead <= 0) {
                break;
            }

            buffer.insert(buffer.end(), tempBuf, tempBuf + bytesRead);

            while (true) {
                if (buffer.size() < 7) {
                    break;
                }

                uint8_t cmd = buffer[0];
                uint16_t topicLen = (static_cast<uint16_t>(buffer[1]) << 8) | static_cast<uint16_t>(buffer[2]);

                if (buffer.size() < 3 + topicLen + 4) {
                    break;
                }

                uint32_t payloadLen = (static_cast<uint32_t>(buffer[3 + topicLen]) << 24) |
                                      (static_cast<uint32_t>(buffer[3 + topicLen + 1]) << 16) |
                                      (static_cast<uint32_t>(buffer[3 + topicLen + 2]) << 8) |
                                       static_cast<uint32_t>(buffer[3 + topicLen + 3]);

                size_t totalLen = 3 + topicLen + 4 + payloadLen;
                if (buffer.size() < totalLen) {
                    break;
                }

                std::string topic(buffer.begin() + 3, buffer.begin() + 3 + topicLen);
                std::string payload(buffer.begin() + 3 + topicLen + 4, buffer.begin() + totalLen);

                if (cmd == static_cast<uint8_t>(Command::Pub) && onMessage) {
                    onMessage(topic, payload);
                }

                buffer.erase(buffer.begin(), buffer.begin() + totalLen);
            }
        }
    });
}

void Subscriber::stop() {
    running_ = false;
    if (socketFd_ >= 0) {
        close(socketFd_);
        socketFd_ = -1;
    }
    if (workerThread_.joinable()) {
        workerThread_.join();
    }
}

Subscriber::~Subscriber() {
    stop();
}

} // namespace nanobus
