# 设备模拟器：压连接数和心跳用。
add_executable(ota_simulator
    simulator_main.cpp
)
target_link_libraries(ota_simulator PRIVATE ota_proto nebula_net spdlog::spdlog ota_build_options Threads::Threads)