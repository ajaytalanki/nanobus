#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace nanobus {

struct MessageFrame {
    std::string topic;
    std::vector<uint8_t> payload;
};

} // namespace nanobus
