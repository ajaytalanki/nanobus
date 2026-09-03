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

} // namespace nanobus::test
