#pragma once

#include "device_control.pb.h"

#include "nebula/net/callbacks.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace nebula::net
{
class Buffer;
class EventLoop;
class TcpServer;
}

namespace ota::gateway
{

namespace net = nebula::net;

struct DeviceSession;
struct GatewayLoopState;

// 超时环形分桶里的一格。
//
// 桶里挂的是「侵入式双向链表」：节点就是 DeviceSession 自己，不是单独的链表节点。
// 好处是挂进桶、从桶里摘下来都只是改几个指针，**不分配、不释放内存**。
// 心跳每秒上万次，用 std::list 的话就是每秒上万次 malloc/free。
struct SessionBucket
{
    DeviceSession* head{nullptr};
    DeviceSession* tail{nullptr};
};

// 一条设备连接的会话状态。
// 连接回调、消息回调、清扫都在它所属的 IO loop 线程里处理，所以这份状态不需要锁。
//
// 所有权在 GatewayLoopState::sessions 的 shared_ptr 上；桶里只放裸指针。
// 对象被销毁时析构函数自动把自己从桶里摘掉，不用手工调 Detach。
struct DeviceSession
{
    net::TcpConnectionPtr conn;
    std::string sn;                                                         // 设备序列号，Hello 之后才有
    std::string model;                                                      // 型号，决定能升哪些固件
    std::string fw_version;                                                 // 设备上报的当前固件版本
    bool authenticated{false};                                              // 有没有通过 Hello 认证

    // 侵入式链表节点：挂在哪个桶里、前后是谁。
    SessionBucket* bucket{nullptr};                                         // nullptr 表示没挂在任何桶里
    DeviceSession* wheel_prev{nullptr};
    DeviceSession* wheel_next{nullptr};

    ~DeviceSession();
};

// 每个 IO loop 一份，存在 EventLoop 的 context 里。
//
// sessions 用哈希表：每条消息都要按连接找会话，线性扫的话 5 万连接时
// 每次心跳要扫上万次，纯浪费。
//
// wheel 是超时用的环形分桶：
//   每秒推进一格，只处理「当前这一格」里的连接。
//   收到消息时把会话从旧桶摘下来、挂到「现在 + 超时秒数」那一格。
//   这里不需要取消语义 —— 重新挂就是取消，所以不用通用定时器那套 slot / generation。
struct GatewayLoopState
{
    std::unordered_map<const net::TcpConnection*, std::shared_ptr<DeviceSession>> sessions;
    std::vector<SessionBucket> wheel;
    std::size_t cursor{0};                                                  // 当前正在处理第几格
};

// 设备长连接网关。
//
// 线程模型：
//   主线程      只跑 accept
//   N 个 IO 线程 各管一批设备连接
class DeviceServer final
{
public:
    // heartbeat_seconds 是服务端要求设备多久发一次；timeout_seconds 是多久收不到消息就当掉线。
    DeviceServer(net::EventLoop* acceptor_loop, const std::string& ip, std::uint16_t port,
                 std::uint32_t heartbeat_seconds = 30, std::uint32_t timeout_seconds = 90);
    ~DeviceServer();

    DeviceServer(const DeviceServer&) = delete;
    DeviceServer& operator=(const DeviceServer&) = delete;

    void Start(std::size_t io_thread_count);

    [[nodiscard]] std::size_t OnlineCount() const;                          // 已认证的设备数
    [[nodiscard]] std::size_t ConnectionCount() const;                      // 当前连接数

private:
    void OnConnection(const net::TcpConnectionPtr& conn);
    void OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer);

    // 只做类型分发，具体逻辑在各自的处理函数里。
    void HandleMessage(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Message& message);

    void HandleHello(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::DeviceHello& hello);
    void HandleHeartbeat(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Heartbeat& heartbeat);

    void Send(const net::TcpConnectionPtr& conn, ota::control::Message& message);

    // 超时分桶：挂进最新的桶、每秒推进一格并踢掉到期连接。
    // 从桶里摘下来由上层的 DetachSession 和 DeviceSession 析构函数负责。
    void AttachSession(GatewayLoopState* state, DeviceSession& session);
    void ScheduleTick(net::EventLoop* loop);
    void TickInLoop(net::EventLoop* loop);

    [[nodiscard]] static DeviceSession* FindSession(GatewayLoopState* state, const net::TcpConnection* conn);
    [[nodiscard]] static std::string DisplaySn(const DeviceSession& session);

    std::unique_ptr<net::TcpServer> server_;

    std::vector<net::EventLoop*> io_loops_;                                 // 启动期写一次，之后只读
    mutable std::mutex loops_mutex_;

    std::uint32_t heartbeat_seconds_{30};                                   // 要求设备多久发一次心跳
    std::uint32_t timeout_seconds_{90};                                     // 多久收不到消息就算掉线

    std::atomic<std::size_t> online_count_{0};                              // 已完成 Hello 的设备数
    std::atomic<std::size_t> connection_count_{0};                          // 当前 TCP 连接数
};

}  // namespace ota::gateway
