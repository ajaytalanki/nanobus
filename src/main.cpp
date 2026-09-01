#include "nanobus/Broker.hpp"

int main() {
    nanobus::Broker broker(8080);
    broker.run();
    return 0;
}
