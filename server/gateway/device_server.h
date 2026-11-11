#pragma once

#include "device_control.pb.h"

#include "server/gateway/device_cache.h"
#include "server/storage/db_executor.h"
#include "server/storage/device_repository.h"

#include "nebula/coro/task.h"
#include "nebula/net/callbacks.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
namespace coro = nebula::coro;

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
    // 这条连接现在处在认证的哪一步。
    enum class AuthState
    {
        kWaitingHello,    // 还没发 Hello
        kAuthenticating,  // Hello 收到，正在查库
        kAuthenticated,   // 认证通过
        kRejected,        // 被拒，等着关连接
    };

    net::TcpConnectionPtr conn;
    std::string sn;                                                                             // 设备序列号，Hello 之后才有
    std::string model;                                                                          // 型号，决定能升哪些固件
    std::string fw_version;                                                                     // 设备上报的当前固件版本
    AuthState auth{AuthState::kWaitingHello};

    // 侵入式链表节点：挂在哪个桶里、前后是谁。
    SessionBucket* bucket{nullptr};                                                             // nullptr 表示没挂在任何桶里
    DeviceSession* wheel_prev{nullptr};
    DeviceSession* wheel_next{nullptr};

    ~DeviceSession();

    [[nodiscard]] bool Authenticated() const noexcept
    {
        return auth == AuthState::kAuthenticated;
    }
};

// 每个 IO loop 一份，存在 EventLoop 的 context 里。
// sessions 是「连接指针 → 会话」的哈希表：每条消息都要按连接找会话，线性扫不起。
// wheel 是超时用的环形分桶，每秒推进一格，收到消息就把会话挂到「现在 + 超时秒数」。
// 这里不需要取消语义 —— 重新挂就是取消，所以不用通用定时器那套 slot / generation。
// 数据库连接和设备缓存也挂在这里，由这一个线程独占，所以全程无锁。
struct GatewayLoopState
{
    std::unordered_map<const net::TcpConnection*, std::shared_ptr<DeviceSession>> sessions;
    std::vector<SessionBucket> wheel;
    std::size_t cursor{0};

    std::unique_ptr<storage::DbExecutor> executor;                                              // 四条按类型分开的 PG 连接
    std::unique_ptr<storage::DeviceRepository> repository;                                      // 设备档案的 SQL 封装
    DeviceCache cache;                                                                          // sn → 设备档案，带 TTL

    // 这一轮需要刷 last_seen_at 的 sn。每 60 秒收集一次，一条 UPDATE 打完就清空。
    // 用 set 而不是 vector：5 万设备每 60 秒可能收到几十万条消息，同一个 sn 不能重复塞。
    std::unordered_set<std::string> pending_last_seen;
};

// 设备长连接网关：所有方法都跑在 IO 线程。
// 构造在主线程只存配置，不碰网络对象；装配由 GatewayHost 负责。
class DeviceServer final
{
public:
    // heartbeat_seconds 是服务端要求设备多久发一次；timeout_seconds 是多久收不到消息就当掉线。
    // refresh_seconds 是 last_seen_at 批量刷库的间隔。
    DeviceServer(storage::DbConfig db_config,
                 std::uint32_t heartbeat_seconds = 30,
                 std::uint32_t timeout_seconds = 90,
                 std::uint32_t refresh_seconds = 60);
    ~DeviceServer();

    DeviceServer(const DeviceServer&) = delete;
    DeviceServer& operator=(const DeviceServer&) = delete;

    // IO 线程起来时调一次：建这个 loop 的会话表、环形分桶、数据库连接和缓存。
    void InitializeLoop(net::EventLoop* loop);

    // 下面两个由 GatewayHost 挂到 TcpServer 上，跑在 IO 线程。
    void OnConnection(const net::TcpConnectionPtr& conn);
    void OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer);

    [[nodiscard]] std::size_t OnlineCount() const;                                              // 已认证的设备数
    [[nodiscard]] std::size_t ConnectionCount() const;                                          // 当前连接数

private:
    // 只做类型分发，具体逻辑在各自的处理函数里。
    void HandleMessage(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Message& message);

    // 同步部分：校验、置状态、查缓存。缓存命中直接放行，未命中才起协程查库。
    void HandleHello(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::DeviceHello& hello);
    void HandleHeartbeat(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Heartbeat& heartbeat);

    // 认证的异步部分：一条直线，没有嵌套回调。
    coro::DetachedTask HandleHelloAsync(net::TcpConnectionPtr conn, std::string sn, std::string token);

    // 认证结果落地：写缓存、回 ServerHello、算在线；失败就拒绝。
    void CompleteAuthentication(const net::TcpConnectionPtr& conn, const storage::AuthResult& auth);
    void RejectConnection(const net::TcpConnectionPtr& conn, const std::string& reason);

    // 异步收尾：批量刷 last_seen、同步设备版本。都是 fire-and-forget。
    coro::DetachedTask RefreshLastSeenAsync(GatewayLoopState* state, std::vector<std::string> sns);
    coro::DetachedTask SyncVersionAsync(GatewayLoopState* state, std::string sn, std::string fw_version);

    void Send(const net::TcpConnectionPtr& conn, ota::control::Message& message);

    // 超时分桶：挂进最新的桶、每秒推进一格并踢掉到期连接。
    void AttachSession(GatewayLoopState* state, DeviceSession& session);
    void ScheduleTick(net::EventLoop* loop);
    void TickInLoop(net::EventLoop* loop);

    // last_seen_at 批量刷库：每 refresh_seconds 秒一次。
    void ScheduleRefresh(net::EventLoop* loop);
    void RefreshInLoop(net::EventLoop* loop);

    [[nodiscard]] static DeviceSession* FindSession(GatewayLoopState* state, const net::TcpConnection* conn);
    [[nodiscard]] static std::string DisplaySn(const DeviceSession& session);

    storage::DbConfig db_config_;
    std::uint32_t heartbeat_seconds_{30};                                                       // 要求设备多久发一次心跳
    std::uint32_t timeout_seconds_{90};                                                         // 多久收不到消息就算掉线
    std::uint32_t refresh_seconds_{60};                                                         // last_seen 批量刷库间隔

    std::atomic<std::size_t> online_count_{0};                                                  // 已完成 Hello 的设备数
    std::atomic<std::size_t> connection_count_{0};                                              // 当前 TCP 连接数
    std::atomic<std::size_t> auth_failed_count_{0};                                             // 认证失败次数，便于观察
};

} // namespace ota::gateway
