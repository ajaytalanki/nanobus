add_executable(router_unit_tests
    RouterTest.cpp
)

target_link_libraries(router_unit_tests
    PRIVATE
        nanobus_core
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(router_unit_tests)

add_executable(connection_unit_tests
    ConnectionTest.cpp
)

target_link_libraries(connection_unit_tests
    PRIVATE
        nanobus_core
        GTest::gtest_main
)

gtest_discover_tests(connection_unit_tests)
