#include "server/gateway_host.h"

#include "nebula/base/logger.h"
#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_server.h"

#include <utility>

namespace ota
{

// 单条连接最多攒 1MB 待发，超了就断，防止卡死的设备把网关拖垮。
constexpr std::size_t kMaxOutputBufferBytes = 1U * 1024U * 1024U;

GatewayHost::GatewayHost(net::EventLoop* acceptor_loop, const std::string& ip, std::uint16_t port,
                         storage::DbConfig db_config,
                         std::uint32_t heartbeat_seconds,
                         std::uint32_t timeout_seconds,
                         std::uint32_t refresh_seconds)
    : server_(std::make_unique<net::TcpServer>(acceptor_loop, ip, port))
    , device_server_(std::make_unique<gateway::DeviceServer>(std::move(db_config),
                                                            heartbeat_seconds,
                                                            timeout_seconds,
                                                            refresh_seconds))
{
    // 三个回调都由 TcpServer 存起来，事件到了再调，所以可以传 lambda。
    server_->SetConnectionCallback(
        [this](const net::TcpConnectionPtr& conn)
        {
            device_server_->OnConnection(conn);
        });

    server_->SetMessageCallback(
        [this](const net::TcpConnectionPtr& conn, net::Buffer* buffer)
        {
            device_server_->OnMessage(conn, buffer);
        });

    // IO 线程起来时，让它给自己建会话表、分桶、数据库连接和缓存。
    server_->SetIoLoopInitCallback(
        [this](net::EventLoop* loop)
        {
            device_server_->InitializeLoop(loop);
        });

    server_->SetMaxOutputBufferBytes(kMaxOutputBufferBytes);
}

GatewayHost::~GatewayHost() = default;

// 开始监听，并拉起指定数量的 IO 线程。
void GatewayHost::Start(std::size_t io_thread_count)
{
    const std::size_t thread_count = io_thread_count == 0 ? 1 : io_thread_count;

    server_->Start(thread_count);

    NLOG_INFO("网关已启动, IO 线程数={}", thread_count);
}

std::size_t GatewayHost::OnlineCount() const
{
    return device_server_->OnlineCount();
}

std::size_t GatewayHost::ConnectionCount() const
{
    return device_server_->ConnectionCount();
}

void GatewayHost::ReportLatency()
{
    device_server_->ReportLatency();
}

}  // namespace ota
