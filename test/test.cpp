#include "nanobus/Client.hpp"

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

int main() {
    nanobus::Subscriber cryptoSub;
    if (!cryptoSub.connect("127.0.0.1", 8080)) {
        std::cerr << "Failed to connect subscriber!\n";
        return 1;
    }

    std::mutex printMutex;
    cryptoSub.listen([&printMutex](const std::string& topic, const std::string& message) {
        std::lock_guard<std::mutex> lock(printMutex);
        std::cout << "[RECEIVED] Topic: " << topic << " | Msg: " << message << "\n";
    });

    cryptoSub.subscribe("market/crypto");

    nanobus::Subscriber stockSub;
    if (!stockSub.connect("127.0.0.1", 8080)) {
        std::cerr << "Failed to connect subscriber!\n";
        return 1;
    }

    stockSub.listen([&printMutex](const std::string& topic, const std::string& message) {
        std::lock_guard<std::mutex> lock(printMutex);
        std::cout << "[RECEIVED] Topic: " << topic << " | Msg: " << message << "\n";
    });

    stockSub.subscribe("market/stocks");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    nanobus::Publisher pub;
    if (!pub.connect("127.0.0.1", 8080)) {
        std::cerr << "Failed to connect publisher!\n";
        return 1;
    }

    pub.publish("market/crypto/btc", "Bitcoin price: $95,000");
    pub.publish("market/crypto/eth", "Ethereum price: $3,500");
    pub.publish("market/stocks/aapl", "Apple stock update");
    pub.publish("market/stocks/tsla", "Tesla stock update");

    std::this_thread::sleep_for(std::chrono::seconds(1));

    return 0;
}
