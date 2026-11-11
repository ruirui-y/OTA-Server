# 服务端：存储层 + 长连接网关。

# storage 必须先于 gateway 创建：gateway 要链接它。
add_subdirectory(storage)
add_subdirectory(gateway)

# 业务服务（service/）以后需要拆出去时再加。

add_executable(ota_server
    main.cpp
    gateway_host.cpp
)
target_link_libraries(ota_server PRIVATE ota_gateway Threads::Threads)