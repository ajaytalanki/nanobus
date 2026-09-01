#pragma once

#include <cstdint>

namespace nanobus {

enum class Command : std::uint8_t {
    Sub = 0x01,
    Unsub = 0x02,
    Pub = 0x03
};

} // namespace nanobus
