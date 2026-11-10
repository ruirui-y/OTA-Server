cmake_minimum_required(VERSION 3.22)

project(OtaServer VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

find_package(Threads REQUIRED)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

# 服务端和模拟器依赖 NebulaRPC，而 NebulaRPC 只能在 Linux 上编（epoll / timerfd）。
# Windows 上只编 Qt 客户端，所以这里按平台决定编哪些子目录。
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OTA_HAS_SERVER ON)
else()
    set(OTA_HAS_SERVER OFF)
endif()

# 所有目标共用的编译选项。
add_library(ota_build_options INTERFACE)

if(MSVC)
    target_compile_options(ota_build_options INTERFACE /W4 /utf-8 /permissive-)
    target_compile_definitions(ota_build_options INTERFACE _CRT_SECURE_NO_WARNINGS)
else()
    target_compile_options(ota_build_options INTERFACE -Wall -Wextra -Wpedantic)
endif()

if(OTA_HAS_SERVER)
    find_package(Protobuf REQUIRED)
    find_package(spdlog REQUIRED)

    if(TARGET protobuf::protoc)
        set(PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
    else()
        find_program(PROTOC_EXECUTABLE NAMES protoc REQUIRED)
    endif()

    set(NEBULARPC_ROOT "$ENV{HOME}/.vs/NebulaRPC" CACHE PATH "NebulaRPC 源码目录")
    if(NOT EXISTS "${NEBULARPC_ROOT}/CMakeLists.txt")
        message(FATAL_ERROR "找不到 NebulaRPC，用 -DNEBULARPC_ROOT=/path/to/NebulaRPC 指定")
    endif()

    # 注意顺序：网关要链接 proto 和 nebula_net，这两个目标必须先创建。
    add_subdirectory("${NEBULARPC_ROOT}" "${CMAKE_BINARY_DIR}/nebularpc" EXCLUDE_FROM_ALL)
    add_subdirectory(proto)
    add_subdirectory(server)
    add_subdirectory(simulator)
endif()

if(WIN32)
    add_subdirectory(client)
endif()