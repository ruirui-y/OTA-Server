# 数据库访问：一条异步 PostgreSQL 连接 + 设备档案 SQL 封装。
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPQ REQUIRED IMPORTED_TARGET libpq>=14)

add_library(ota_storage STATIC
    db_connection.cpp
    db_executor.cpp
    device_repository.cpp
)
target_include_directories(ota_storage PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(ota_storage PUBLIC nebula_net PkgConfig::LIBPQ ota_build_options Threads::Threads)