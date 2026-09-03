include(GoogleTest)

function(add_nanobus_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target}
        PRIVATE
            nanobus_core
            GTest::gtest_main
    )
    gtest_discover_tests(${target})
endfunction()

add_nanobus_test(router_unit_tests RouterTest.cpp)
add_nanobus_test(connection_unit_tests ConnectionTest.cpp)
add_nanobus_test(broker_integration_tests BrokerIntegrationTest.cpp)
