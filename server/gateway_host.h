#pragma once

#include "server/gateway/device_server.h"
#include "server/storage/db_connection.h"

#include "nebula/net/callbacks.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace nebula::net
{
class EventLoop;
class TcpServer;
}

namespace ota
{

namespace net = nebula::net;

// 网关的宿主：只管装配，自己跑在主线程。
// 建 TcpServer、把 DeviceServer 的回调挂上去、拉起 IO 线程，然后把控制权交给 acceptor loop。
//
// 线程：这个类的所有方法都在主线程；挂上去的回调则都在 IO 线程里跑。
// 目录约定：server/ 顶层 = 主线程，server/gateway/ 和 server/storage/ = IO 线程。
class GatewayHost final
{
public:
    GatewayHost(net::EventLoop* acceptor_loop, const std::string& ip, std::uint16_t port,
                storage::DbConfig db_config,
                std::uint32_t heartbeat_seconds = 30,
                std::uint32_t timeout_seconds = 90,
                std::uint32_t refresh_seconds = 60);
    ~GatewayHost();

    GatewayHost(const GatewayHost&) = delete;
    GatewayHost& operator=(const GatewayHost&) = delete;

    // 开始监听，并拉起 io_thread_count 条 IO 线程。
    void Start(std::size_t io_thread_count);

    [[nodiscard]] std::size_t OnlineCount() const;                         // 已认证的设备数
    [[nodiscard]] std::size_t ConnectionCount() const;                     // 当前连接数

private:
    std::unique_ptr<net::TcpServer> server_;                               // 网络层，归主线程所有
    std::unique_ptr<gateway::DeviceServer> device_server_;                 // 回调处理，方法都跑在 IO 线程
};

}  // namespace ota
