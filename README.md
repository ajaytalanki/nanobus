# nanobus

A lightweight in-process pub/sub broker written in C++ using a custom TCP wire protocol.

## Project structure

```text
nanobus/
├── CMakeLists.txt
├── include/
│   └── nanobus/
│       ├── Broker.hpp
│       ├── Client.hpp
│       ├── Connection.hpp
│       ├── MessageFrame.hpp
│       ├── Protocol.hpp
│       └── Router.hpp
├── src/
│   ├── Broker.cpp
│   ├── Client.cpp
│   ├── Connection.cpp
│   ├── main.cpp
│   └── Router.cpp
├── test/
│   └── test.cpp
├── build/
└── README.md
```

## What this project does

- Starts a TCP broker on port 8080
- Accepts subscriber connections
- Supports topic prefix subscriptions like `market/crypto`
- Publishes messages to matched subscribers
- Uses an epoll-based event loop for scalable socket handling

## Build

### Option 1: direct compiler build

```bash
cd /mnt/c/Users/ajays/OneDrive/Desktop/Projects/nanobus
mkdir -p build
g++ -std=c++2a -pthread -Iinclude src/*.cpp -o build/nanobus
g++ -std=c++2a -pthread -Iinclude src/Client.cpp src/Connection.cpp src/Router.cpp src/Broker.cpp test/test.cpp -o build/test_app
```

### Option 2: CMake (preferred when installed)

```bash
cd /mnt/c/Users/ajays/OneDrive/Desktop/Projects/nanobus
cmake -S . -B build
cmake --build build
```

## Run

Start the broker:

```bash
./build/nanobus
```

Run the sample publisher/subscriber test:

```bash
./build/test_app
```

## Wire protocol

Each frame is encoded as:

```text
[1 byte command][2 bytes topic length][N bytes topic][4 bytes payload length][M bytes payload]
```

Supported commands:

- `0x01` = subscribe
- `0x02` = unsubscribe
- `0x03` = publish

The broker matches published topics against subscriber topic prefixes using prefix-based matching.

Example:

- Subscriber subscribes to `market/crypto`
- Publisher sends to `market/crypto/btc`
- Message is routed to the subscriber

## Notes

- The current implementation is a simple educational broker and not production-hardened.
- The code uses a single-threaded epoll loop for broker coordination and background threads for subscriber callbacks.
- The sample test intentionally locks around `std::cout` to prevent interleaved log output from multiple async callbacks.
