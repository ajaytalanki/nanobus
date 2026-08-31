#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

enum Command : uint8_t {
    cmdSub   = 0x01,
    cmdUnsub = 0x02,
    cmdPub   = 0x03
};

// Base Client class handling connection and wire protocol encoding
class Client {
protected:
    int socketFd = -1;

    bool connectToBroker(const std::string& host, int port) {
        socketFd = socket(AF_INET, SOCK_STREAM, 0);
        if (socketFd < 0) return false;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        return connect(socketFd, (struct sockaddr*)&addr, sizeof(addr)) == 0;
    }

    void sendFrame(Command cmd, const std::string& topic, const std::string& payload = "") {
        std::vector<uint8_t> frame;
        uint16_t topicLen = htons(topic.size());

        frame.push_back(cmd);
        frame.insert(frame.end(), (uint8_t*)&topicLen, (uint8_t*)&topicLen + 2);
        frame.insert(frame.end(), topic.begin(), topic.end());

        if (cmd == cmdPub) {
            uint32_t payloadLen = htonl(payload.size());
            frame.insert(frame.end(), (uint8_t*)&payloadLen, (uint8_t*)&payloadLen + 4);
            frame.insert(frame.end(), payload.begin(), payload.end());
        }

        send(socketFd, frame.data(), frame.size(), 0);
    }

public:
    virtual ~Client() {
        if (socketFd >= 0) close(socketFd);
    }
};

// --- Publisher Class ---
class Publisher : public Client {
public:
    bool connect(const std::string& host = "127.0.0.1", int port = 8080) {
        return connectToBroker(host, port);
    }

    void publish(const std::string& topic, const std::string& message) {
        sendFrame(cmdPub, topic, message);
    }
};

// --- Subscriber Class ---
class Subscriber : public Client {
private:
    std::thread workerThread;
    std::atomic<bool> running{false};

public:
    bool connect(const std::string& host = "127.0.0.1", int port = 8080) {
        return connectToBroker(host, port);
    }

    void subscribe(const std::string& topicPrefix) {
        sendFrame(cmdSub, topicPrefix);
    }

    void unsubscribe(const std::string& topicPrefix) {
        sendFrame(cmdUnsub, topicPrefix);
    }

    // Listens asynchronously in a background thread and triggers a callback on message
    void listen(std::function<void(const std::string& topic, const std::string& payload)> onMessage) {
        running = true;
        workerThread = std::thread([this, onMessage]() {
            std::vector<uint8_t> buffer;
            uint8_t tempBuf[4096];

            while (running) {
                ssize_t bytesRead = recv(socketFd, tempBuf, sizeof(tempBuf), 0);
                if (bytesRead <= 0) break; // Disconnected or error

                buffer.insert(buffer.end(), tempBuf, tempBuf + bytesRead);

                // Parse incoming frames
                while (true) {
                    if (buffer.size() < 7) break; // Min header size for PUB frame
                    
                    uint8_t cmd = buffer[0];
                    uint16_t topicLen = (buffer[1] << 8) | buffer[2];
                    
                    if (buffer.size() < 3 + topicLen + 4) break;
                    
                    uint32_t payloadLen = (buffer[3 + topicLen] << 24) |
                                          (buffer[3 + topicLen + 1] << 16) |
                                          (buffer[3 + topicLen + 2] << 8) |
                                           buffer[3 + topicLen + 3];

                    size_t totalLen = 3 + topicLen + 4 + payloadLen;
                    if (buffer.size() < totalLen) break; // Wait for full frame

                    std::string topic(buffer.begin() + 3, buffer.begin() + 3 + topicLen);
                    std::string payload(buffer.begin() + 3 + topicLen + 4, buffer.begin() + totalLen);

                    if (cmd == cmdPub && onMessage) {
                        onMessage(topic, payload);
                    }

                    buffer.erase(buffer.begin(), buffer.begin() + totalLen);
                }
            }
        });
    }

    void stop() {
        running = false;
        if (socketFd >= 0) close(socketFd);
        if (workerThread.joinable()) workerThread.join();
    }

    ~Subscriber() {
        stop();
    }
};