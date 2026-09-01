#pragma once

#include <vector>
#include <cstdint>

namespace nanobus {

struct Connection {
    int fd;
    std::vector<uint8_t> readBuf;
    std::vector<uint8_t> writeBuf;

    explicit Connection(int socketFd);
    void setNonblocking();
};

} // namespace nanobus
