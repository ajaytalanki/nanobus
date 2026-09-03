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
