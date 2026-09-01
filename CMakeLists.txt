cmake_minimum_required(VERSION 3.16)

project(nanobus LANGUAGES CXX)

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

add_executable(nanobus
    src/main.cpp
)

target_link_libraries(nanobus PRIVATE nanobus_core)

add_executable(test_app
    test/test.cpp
)

target_link_libraries(test_app PRIVATE nanobus_core)
