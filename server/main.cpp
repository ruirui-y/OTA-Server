// 服务端入口：起设备长连接网关，然后定期打在线统计。
//
// 设计是「两线程」：主线程只跑 accept，一条 IO 线程管所有设备连接和那条数据库连接。
// 所以 --io 默认是 1。

#include "server/gateway_host.h"

#include "nebula/net/event_loop.h"

#include "nebula/base/logger.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

namespace
{

namespace net = nebula::net;

// 参数不对时打印用法。
void PrintUsage()
{
    std::printf("用法: ota_server [选项]\n");
    std::printf("  --listen 端口        监听端口，默认 9500\n");
    std::printf("  --io N               处理设备连接的 IO 线程数，默认 1\n");
    std::printf("  --heartbeat 秒       要求设备多久发一次心跳，默认 30\n");
    std::printf("  --timeout 秒         多久收不到消息算掉线，默认 90\n");
    std::printf("  --refresh 秒         last_seen 批量刷库间隔，默认 60\n");
    std::printf("  --db-host 主机       PostgreSQL 主机，默认 127.0.0.1\n");
    std::printf("  --db-port 端口       PostgreSQL 端口，默认 5432\n");
    std::printf("  --db-user 用户       PostgreSQL 用户，默认 dev\n");
    std::printf("  --db-password 密码   PostgreSQL 密码，默认 123456\n");
    std::printf("  --db-name 库名       PostgreSQL 库名，默认 ota_server\n");
    std::printf("  --help               打印这段说明\n");
}

} // namespace

int main(int argc, char** argv)
{
    std::uint16_t listen_port = 9500;
    int io_threads = 1;
    std::uint32_t heartbeat_seconds = 30;
    std::uint32_t timeout_seconds = 90;
    std::uint32_t refresh_seconds = 60;

    ota::storage::DbConfig db_config;

    for (int i = 1; i < argc; ++i)
    {
        const char* arg = argv[i];

        if (std::strcmp(arg, "--help") == 0)
        {
            PrintUsage();
            return 0;
        }

        if (i + 1 >= argc)
        {
            std::printf("参数 %s 后面缺一个值\n", arg);
            PrintUsage();
            return 1;
        }

        const char* value = argv[++i];

        if (std::strcmp(arg, "--listen") == 0)
        {
            listen_port = static_cast<std::uint16_t>(std::atoi(value));
        }
        else if (std::strcmp(arg, "--io") == 0)
        {
            io_threads = std::atoi(value);
        }
        else if (std::strcmp(arg, "--heartbeat") == 0)
        {
            heartbeat_seconds = static_cast<std::uint32_t>(std::atoi(value));
        }
        else if (std::strcmp(arg, "--timeout") == 0)
        {
            timeout_seconds = static_cast<std::uint32_t>(std::atoi(value));
        }
        else if (std::strcmp(arg, "--refresh") == 0)
        {
            refresh_seconds = static_cast<std::uint32_t>(std::atoi(value));
        }
        else if (std::strcmp(arg, "--db-host") == 0)
        {
            db_config.host = value;
        }
        else if (std::strcmp(arg, "--db-port") == 0)
        {
            db_config.port = static_cast<std::uint16_t>(std::atoi(value));
        }
        else if (std::strcmp(arg, "--db-user") == 0)
        {
            db_config.user = value;
        }
        else if (std::strcmp(arg, "--db-password") == 0)
        {
            db_config.password = value;
        }
        else if (std::strcmp(arg, "--db-name") == 0)
        {
            db_config.database = value;
        }
        else
        {
            std::printf("不认识的参数: %s\n", arg);
            PrintUsage();
            return 1;
        }
    }

    if (io_threads <= 0)
    {
        io_threads = 1;
    }

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

    NLOG_INFO("======== ota_server 启动 ========");
    NLOG_INFO("监听={} IO 线程={}", listen_port, io_threads);
    NLOG_INFO("数据库={}:{}/{}", db_config.host, db_config.port, db_config.database);

    // 主线程只跑 accept。
    net::EventLoop acceptor_loop;

    ota::GatewayHost gateway(&acceptor_loop,
                             "0.0.0.0",
                             listen_port,
                             db_config,
                             heartbeat_seconds,
                             timeout_seconds,
                             refresh_seconds);

    gateway.Start(static_cast<std::size_t>(io_threads));

    std::printf("网关监听         : 0.0.0.0:%u\n", static_cast<unsigned>(listen_port));
    std::printf("IO 线程          : %d\n", io_threads);
    std::printf("心跳 / 超时      : %u s / %u s\n", heartbeat_seconds, timeout_seconds);
    std::printf("last_seen 刷库   : 每 %u 秒\n", refresh_seconds);
    std::printf("数据库           : %s:%u/%s\n",
                db_config.host.c_str(),
                static_cast<unsigned>(db_config.port),
                db_config.database.c_str());

    // 单独一条线程定期打印统计，不在 IO 线程里做。
    std::thread reporter(
        [&gateway]
        {
            for (;;)
            {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                NLOG_INFO("连接={} 在线={}", gateway.ConnectionCount(), gateway.OnlineCount());
            }
        });
    reporter.detach();

    acceptor_loop.Loop();
    return 0;
}
