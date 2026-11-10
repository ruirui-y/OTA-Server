# 设备长连接网关：收帧、认证、心跳。
add_library(ota_gateway STATIC
    device_server.cpp
)
target_include_directories(ota_gateway PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(ota_gateway PUBLIC ota_proto nebula_net spdlog::spdlog ota_build_options Threads::Threads)