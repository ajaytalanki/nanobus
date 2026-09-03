#include "nanobus/Broker.hpp"
#include "nanobus/Client.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <mutex>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace nanobus::test {

constexpr int brokerPort = 8080;

bool waitForBroker() {
    for (int attempt = 0; attempt < 50; ++attempt) {
        int socketFd = socket(AF_INET, SOCK_STREAM, 0);
        if (socketFd >= 0) {
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_port = htons(brokerPort);
            inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

            bool connected = connect(
                socketFd,
                reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == 0;
            close(socketFd);
            if (connected) {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

std::vector<uint8_t> makePublishFrame(const std::string& topic,
                                      const std::string& payload) {
    std::vector<uint8_t> frame;
    uint16_t topicLength = htons(static_cast<uint16_t>(topic.size()));
    uint32_t payloadLength = htonl(static_cast<uint32_t>(payload.size()));

    frame.push_back(static_cast<uint8_t>(nanobus::Command::Pub));
    frame.insert(frame.end(), reinterpret_cast<uint8_t*>(&topicLength),
                 reinterpret_cast<uint8_t*>(&topicLength) + sizeof(topicLength));
    frame.insert(frame.end(), topic.begin(), topic.end());
    frame.insert(frame.end(), reinterpret_cast<uint8_t*>(&payloadLength),
                 reinterpret_cast<uint8_t*>(&payloadLength) + sizeof(payloadLength));
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

int connectRawPublisher() {
    int socketFd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0) {
        return -1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(brokerPort);
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

    if (connect(socketFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        close(socketFd);
        return -1;
    }
    return socketFd;
}

bool sendAll(int socketFd, const std::vector<uint8_t>& data) {
    std::size_t bytesSent = 0;
    while (bytesSent < data.size()) {
        ssize_t result = send(socketFd, data.data() + bytesSent,
                              data.size() - bytesSent, 0);
        if (result <= 0) {
            return false;
        }
        bytesSent += static_cast<std::size_t>(result);
    }
    return true;
}

class BrokerIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        brokerProcess_ = fork();
        ASSERT_NE(brokerProcess_, -1);

        if (brokerProcess_ == 0) {
            nanobus::Broker broker(brokerPort);
            broker.run();
            _exit(0);
        }

        ASSERT_TRUE(waitForBroker());
    }

    void TearDown() override {
        if (brokerProcess_ > 0) {
            kill(brokerProcess_, SIGTERM);
            waitpid(brokerProcess_, nullptr, 0);
        }
    }

private:
    pid_t brokerProcess_ = -1;
};

TEST_F(BrokerIntegrationTest, DeliversMessageOnlyToExactTopicSubscriber) {
    nanobus::Subscriber matchingSubscriber;
    nanobus::Subscriber nonMatchingSubscriber;
    ASSERT_TRUE(matchingSubscriber.connect("127.0.0.1", brokerPort));
    ASSERT_TRUE(nonMatchingSubscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messageReceived;
    std::vector<std::pair<std::string, std::string>> matchingMessages;
    std::vector<std::pair<std::string, std::string>> nonMatchingMessages;

    matchingSubscriber.listen([&](const std::string& topic, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            matchingMessages.emplace_back(topic, payload);
        }
        messageReceived.notify_one();
    });
    nonMatchingSubscriber.listen([&](const std::string& topic, const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex);
        nonMatchingMessages.emplace_back(topic, payload);
    });

    matchingSubscriber.subscribe("market/crypto/btc");
    nonMatchingSubscriber.subscribe("market/crypto/eth");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish("market/crypto/btc", "Bitcoin price: $95,000");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(messageReceived.wait_for(lock, std::chrono::seconds(1), [&] {
            return matchingMessages.size() == 1;
        }));

        EXPECT_EQ(matchingMessages[0].first, "market/crypto/btc");
        EXPECT_EQ(matchingMessages[0].second, "Bitcoin price: $95,000");
        EXPECT_TRUE(nonMatchingMessages.empty());
    }
}

TEST_F(BrokerIntegrationTest, DeliversMultipleMessagesInPublishOrder) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messagesReceived;
    std::vector<std::string> receivedMessages;

    subscriber.listen([&](const std::string&, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.push_back(payload);
        }
        messagesReceived.notify_one();
    });
    subscriber.subscribe("market/crypto/btc");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish("market/crypto/btc", "Bitcoin price: $95,000");
    publisher.publish("market/crypto/btc", "Bitcoin price: $96,000");
    publisher.publish("market/crypto/btc", "Bitcoin price: $97,000");
    publisher.publish("market/crypto/btc", "Bitcoin price: $98,000");
    publisher.publish("market/crypto/btc", "Bitcoin price: $99,000");

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messagesReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedMessages.size() == 5;
    }));

    EXPECT_EQ(receivedMessages, (std::vector<std::string>{
        "Bitcoin price: $95,000",
        "Bitcoin price: $96,000",
        "Bitcoin price: $97,000",
        "Bitcoin price: $98,000",
        "Bitcoin price: $99,000",
    }));
}

TEST_F(BrokerIntegrationTest, FansOutMessageToMultipleSubscribers) {
    constexpr std::size_t subscriberCount = 4;
    std::vector<std::unique_ptr<nanobus::Subscriber>> subscribers;
    subscribers.reserve(subscriberCount);

    std::mutex mutex;
    std::condition_variable messagesReceived;
    std::vector<std::vector<std::string>> receivedMessages(subscriberCount);

    for (std::size_t index = 0; index < subscriberCount; ++index) {
        auto subscriber = std::make_unique<nanobus::Subscriber>();
        ASSERT_TRUE(subscriber->connect("127.0.0.1", brokerPort));
        subscriber->listen([&, index](const std::string&, const std::string& payload) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                receivedMessages[index].push_back(payload);
            }
            messagesReceived.notify_one();
        });
        subscriber->subscribe("market/crypto/btc");
        subscribers.push_back(std::move(subscriber));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish("market/crypto/btc", "Bitcoin price: $95,000");

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messagesReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        for (const auto& messages : receivedMessages) {
            if (messages.size() != 1) {
                return false;
            }
        }
        return true;
    }));

    for (const auto& messages : receivedMessages) {
        ASSERT_EQ(messages.size(), 1);
        EXPECT_EQ(messages[0], "Bitcoin price: $95,000");
    }
}

TEST_F(BrokerIntegrationTest, OneSubscriberReceivesMultipleTopics) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messagesReceived;
    std::vector<std::pair<std::string, std::string>> receivedMessages;

    subscriber.listen([&](const std::string& topic, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.emplace_back(topic, payload);
        }
        messagesReceived.notify_one();
    });
    subscriber.subscribe("market/crypto/btc");
    subscriber.subscribe("market/stocks/aapl");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish("market/crypto/btc", "Bitcoin price: $95,000");
    publisher.publish("market/stocks/aapl", "Apple stock update");

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messagesReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedMessages.size() == 2;
    }));

    EXPECT_EQ(receivedMessages, (std::vector<std::pair<std::string, std::string>>{
        {"market/crypto/btc", "Bitcoin price: $95,000"},
        {"market/stocks/aapl", "Apple stock update"},
    }));
}

TEST_F(BrokerIntegrationTest, UnsubscribeStopsFutureDelivery) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messageReceived;
    std::vector<std::string> receivedMessages;

    subscriber.listen([&](const std::string&, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.push_back(payload);
        }
        messageReceived.notify_one();
    });

    constexpr const char* topic = "market/crypto/btc";
    subscriber.subscribe(topic);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish(topic, "before unsubscribe");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(messageReceived.wait_for(lock, std::chrono::seconds(1), [&] {
            return receivedMessages.size() == 1;
        }));
    }

    subscriber.unsubscribe(topic);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    publisher.publish(topic, "after unsubscribe");

    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(receivedMessages.size(), 1);
    EXPECT_EQ(receivedMessages[0], "before unsubscribe");
}

TEST_F(BrokerIntegrationTest, DeliversEmptyPayload) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messageReceived;
    std::vector<std::pair<std::string, std::string>> receivedMessages;

    subscriber.listen([&](const std::string& topic, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.emplace_back(topic, payload);
        }
        messageReceived.notify_one();
    });
    subscriber.subscribe("events/heartbeat");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish("events/heartbeat", "");

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messageReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedMessages.size() == 1;
    }));

    ASSERT_EQ(receivedMessages.size(), 1);
    EXPECT_EQ(receivedMessages[0].first, "events/heartbeat");
    EXPECT_TRUE(receivedMessages[0].second.empty());
}

TEST_F(BrokerIntegrationTest, DeliversBinaryPayloadWithoutTruncation) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messageReceived;
    std::vector<std::string> receivedPayloads;

    subscriber.listen([&](const std::string&, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedPayloads.push_back(payload);
        }
        messageReceived.notify_one();
    });
    subscriber.subscribe("events/binary");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const std::string binaryPayload("\x00\x01\xff", 3);
    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish("events/binary", binaryPayload);

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messageReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedPayloads.size() == 1;
    }));

    ASSERT_EQ(receivedPayloads.size(), 1);
    ASSERT_EQ(receivedPayloads[0].size(), 3);
    EXPECT_EQ(static_cast<unsigned char>(receivedPayloads[0][0]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(receivedPayloads[0][1]), 0x01);
    EXPECT_EQ(static_cast<unsigned char>(receivedPayloads[0][2]), 0xff);
}

TEST_F(BrokerIntegrationTest, HandlesPublishFrameSplitAcrossWrites) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messageReceived;
    std::vector<std::string> receivedMessages;
    subscriber.listen([&](const std::string&, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.push_back(payload);
        }
        messageReceived.notify_one();
    });
    subscriber.subscribe("events/split");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int publisherSocket = connectRawPublisher();
    ASSERT_NE(publisherSocket, -1);
    const auto frame = makePublishFrame("events/split", "split frame payload");
    ASSERT_GT(send(publisherSocket, frame.data(), 2, 0), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(sendAll(publisherSocket,
                        std::vector<uint8_t>(frame.begin() + 2, frame.end())));
    close(publisherSocket);

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messageReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedMessages.size() == 1;
    }));
    EXPECT_EQ(receivedMessages[0], "split frame payload");
}

TEST_F(BrokerIntegrationTest, HandlesMultiplePublishFramesInOneWrite) {
    nanobus::Subscriber subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messagesReceived;
    std::vector<std::string> receivedMessages;
    subscriber.listen([&](const std::string&, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.push_back(payload);
        }
        messagesReceived.notify_one();
    });
    subscriber.subscribe("events/batch");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const auto firstFrame = makePublishFrame("events/batch", "first");
    const auto secondFrame = makePublishFrame("events/batch", "second");
    std::vector<uint8_t> frames = firstFrame;
    frames.insert(frames.end(), secondFrame.begin(), secondFrame.end());

    int publisherSocket = connectRawPublisher();
    ASSERT_NE(publisherSocket, -1);
    ASSERT_TRUE(sendAll(publisherSocket, frames));
    close(publisherSocket);

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messagesReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedMessages.size() == 2;
    }));
    EXPECT_EQ(receivedMessages, (std::vector<std::string>{"first", "second"}));
}

TEST_F(BrokerIntegrationTest, RemovesDisconnectedSubscriber) {
    nanobus::Subscriber disconnectedSubscriber;
    nanobus::Subscriber activeSubscriber;
    ASSERT_TRUE(disconnectedSubscriber.connect("127.0.0.1", brokerPort));
    ASSERT_TRUE(activeSubscriber.connect("127.0.0.1", brokerPort));

    std::mutex mutex;
    std::condition_variable messageReceived;
    std::vector<std::string> receivedMessages;
    activeSubscriber.listen([&](const std::string&, const std::string& payload) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            receivedMessages.push_back(payload);
        }
        messageReceived.notify_one();
    });

    constexpr const char* topic = "events/disconnect";
    disconnectedSubscriber.subscribe(topic);
    activeSubscriber.subscribe(topic);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    disconnectedSubscriber.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", brokerPort));
    publisher.publish(topic, "active subscriber only");

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(messageReceived.wait_for(lock, std::chrono::seconds(1), [&] {
        return receivedMessages.size() == 1;
    }));
    EXPECT_EQ(receivedMessages[0], "active subscriber only");
}

} // namespace nanobus::test
