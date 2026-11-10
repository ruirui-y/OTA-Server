// 服务端入口：起设备长连接网关，然后每 5 秒打一次在线统计。

#include "server/gateway/device_server.h"

#include "nebula/net/event_loop.h"

#include "nebula/base/logger.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>

namespace
{

namespace net = nebula::net;

// 参数不对时打印用法。
void PrintUsage()
{
    std::printf("用法: ota_server [--listen 9500] [--io 4]\n");
    std::printf("  --listen  监听端口，默认 9500\n");
    std::printf("  --io      处理设备连接的 IO 线程数，默认 4\n");
}

}  // namespace

int main(int argc, char** argv)
{
    std::uint16_t listen_port = 9500;
    int io_threads = 4;

    for (int i = 1; i < argc; ++i)
    {
        if (std::strcmp(argv[i], "--listen") == 0 && i + 1 < argc)
        {
            listen_port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        }
        else if (std::strcmp(argv[i], "--io") == 0 && i + 1 < argc)
        {
            io_threads = std::atoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "--help") == 0)
        {
            PrintUsage();
            return 0;
        }
    }

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

    // 主线程只跑 accept。
    net::EventLoop acceptor_loop;

    ota::gateway::DeviceServer server(&acceptor_loop, "0.0.0.0", listen_port);
    server.Start(static_cast<std::size_t>(io_threads));

    std::printf("网关监听         : 0.0.0.0:%u\n", static_cast<unsigned>(listen_port));
    std::printf("IO 线程          : %d\n", io_threads);

    // 单独一条线程定期打印统计，不在 IO 线程里做。
    std::thread reporter(
        [&server]
        {
            for (;;)
            {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                NLOG_INFO("连接={} 在线={}", server.ConnectionCount(), server.OnlineCount());
            }
        });
    reporter.detach();

    acceptor_loop.Loop();
    return 0;
}