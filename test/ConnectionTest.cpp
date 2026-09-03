#include "nanobus/Connection.hpp"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace nanobus::test {

TEST(ConnectionTest, InitializesSocketAndBuffers) {
    Connection connection(123);

    EXPECT_EQ(connection.fd, 123);
    EXPECT_TRUE(connection.readBuf.empty());
    EXPECT_TRUE(connection.writeBuf.empty());
}

TEST(ConnectionTest, SetsSocketToNonblocking) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

    Connection connection(sockets[0]);
    connection.setNonblocking();

    int flags = fcntl(sockets[0], F_GETFL, 0);
    ASSERT_NE(flags, -1);
    EXPECT_NE(flags & O_NONBLOCK, 0);

    close(sockets[0]);
    close(sockets[1]);
}

} // namespace nanobus::test
