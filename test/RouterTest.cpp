#include "nanobus/Router.hpp"

#include <gtest/gtest.h>

namespace nanobus::test {

TEST(RouterTest, MatchesExactTopics) {
    nanobus::Router router;
    router.subscribe(30, "market");
    router.subscribe(10, "market/crypto/btc");
    router.subscribe(20, "orders");

    EXPECT_EQ(router.getMatchedSubscribers("market/crypto/btc"), (std::vector<int>{10}));
    EXPECT_EQ(router.getMatchedSubscribers("orders"), (std::vector<int>{20}));
    EXPECT_TRUE(router.getMatchedSubscribers("metrics/cpu").empty());
}

TEST(RouterTest, DoesNotMatchSimilarTopicNames) {
    nanobus::Router router;
    router.subscribe(7, "market");

    EXPECT_TRUE(router.getMatchedSubscribers("marketplace").empty());
    EXPECT_TRUE(router.getMatchedSubscribers("market/crypto").empty());
    EXPECT_EQ(router.getMatchedSubscribers("market"), (std::vector<int>{7}));
}

TEST(RouterTest, OneClientCanSubscribeToMultipleTopics) {
    nanobus::Router router;
    router.subscribe(42, "market/crypto/eth");
    router.subscribe(42, "market/stocks/aapl");

    EXPECT_EQ(router.getMatchedSubscribers("market/crypto/eth"), (std::vector<int>{42}));
    EXPECT_EQ(router.getMatchedSubscribers("market/stocks/aapl"), (std::vector<int>{42}));
}

TEST(RouterTest, DuplicateSubscriptionsDoNotDuplicateDelivery) {
    nanobus::Router router;
    router.subscribe(7, "market");
    router.subscribe(7, "market");
    router.subscribe(7, "market/crypto/btc");

    EXPECT_EQ(router.getMatchedSubscribers("market"), (std::vector<int>{7}));
}

TEST(RouterTest, UnsubscribeRemovesOnlyRequestedTopic) {
    nanobus::Router router;
    router.subscribe(5, "market/crypto/btc");
    router.subscribe(5, "market/stocks/aapl");
    router.unsubscribe(5, "market/crypto/btc");

    EXPECT_TRUE(router.getMatchedSubscribers("market/crypto/btc").empty());
    EXPECT_EQ(router.getMatchedSubscribers("market/stocks/aapl"), (std::vector<int>{5}));
}

TEST(RouterTest, RemoveClientCleansAllSubscriptions) {
    nanobus::Router router;
    router.subscribe(11, "market/crypto/btc");
    router.subscribe(11, "orders");
    router.subscribe(12, "market/crypto/btc");

    router.removeClient(11);

    EXPECT_EQ(router.getMatchedSubscribers("market/crypto/btc"), (std::vector<int>{12}));
    EXPECT_TRUE(router.getMatchedSubscribers("orders").empty());
}

TEST(RouterTest, ResultsAreSorted) {
    nanobus::Router router;
    router.subscribe(30, "market");
    router.subscribe(10, "market");
    router.subscribe(20, "market");

    EXPECT_EQ(router.getMatchedSubscribers("market"),
              (std::vector<int>{10, 20, 30}));
}

} // namespace nanobus::test
