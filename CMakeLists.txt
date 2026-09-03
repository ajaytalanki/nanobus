cmake_minimum_required(VERSION 3.16)

project(nanobus LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
enable_testing()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(nanobus_core
    src/Connection.cpp
    src/Router.cpp
    src/Client.cpp
    src/Broker.cpp
)

target_include_directories(nanobus_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(nanobus_core PUBLIC cxx_std_20)
target_link_libraries(nanobus_core PUBLIC Threads::Threads)

add_executable(nanobus
    src/main.cpp
)

target_link_libraries(nanobus PRIVATE nanobus_core)

add_subdirectory(test)

add_custom_target(test
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS router_unit_tests connection_unit_tests
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
