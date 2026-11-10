# 服务端：长连接网关 + 业务服务 + 存储。
add_subdirectory(gateway)

# 网关先编出来，业务服务和存储后面阶段再加。

add_executable(ota_server
    main.cpp
)
target_link_libraries(ota_server PRIVATE ota_gateway Threads::Threads)