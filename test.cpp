#include "client.hpp"
#include <iostream>
#include <chrono>
#include <thread>

int main() {
    // 1. Create and start a Subscriber
    Subscriber sub;
    if (!sub.connect("127.0.0.1", 8080)) {
        std::cerr << "Failed to connect subscriber!\n";
        return 1;
    }

    // Register callback for incoming messages
    sub.listen([](const std::string& topic, const std::string& message) {
        std::cout << "[RECEIVED] Topic: " << topic << " | Msg: " << message << "\n";
    });

    // Subscribe to a topic prefix
    sub.subscribe("market/crypto");
    std::cout << "Subscribed to 'market/crypto'...\n";

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 2. Create a Publisher and send messages
    Publisher pub;
    if (!pub.connect("127.0.0.1", 8080)) {
        std::cerr << "Failed to connect publisher!\n";
        return 1;
    }

    std::cout << "Publishing messages...\n";
    pub.publish("market/crypto/btc", "Bitcoin price: $95,000");
    pub.publish("market/crypto/eth", "Ethereum price: $3,500");
    pub.publish("market/stocks/aapl", "Apple stock update"); // Will be ignored by sub (different prefix)

    // Keep main thread alive briefly to process received messages
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return 0;
}