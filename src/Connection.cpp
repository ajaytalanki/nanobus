#include "nanobus/Connection.hpp"

#include <fcntl.h>
#include <unistd.h>

namespace nanobus {

Connection::Connection(int socketFd) : fd(socketFd) {}

void Connection::setNonblocking() {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

} // namespace nanobus
