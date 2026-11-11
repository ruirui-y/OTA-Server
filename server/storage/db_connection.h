#pragma once

#include "nebula/net/callbacks.h"

#include <libpq-fe.h>

#include <coroutine>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

namespace nebula::net
{
class Channel;
class EventLoop;
}

namespace ota::storage
{

namespace net = nebula::net;                                                                    // 下面统一写 net::，不用带 nebula 前缀

// 连哪个库。
// 一个 DbExecutor 里的四条连接共用这一份配置。
struct DbConfig
{
    std::string host{"127.0.0.1"};
    std::uint16_t port{5432};
    std::string user{"dev"};
    std::string password{"123456"};
    std::string database{"ota_server"};
};

class QueryAwaiter;                                                                             // 定义在 db_awaiter.h

// PGresult 的自有指针：析构自动 PQclear。
using PgResult = std::unique_ptr<PGresult, decltype(&PQclear)>;

// 一条 Pipeline 模式下的 PostgreSQL 连接。
// 任何一步都不阻塞 EventLoop：发送只写 libpq 缓冲，结果由 socket 事件驱动。
// 一个同步点之间的 SQL 属于同一个隐式事务，所以结果要等同步点到了才交付。
// 要发查询用 db_awaiter.h 里的 CoQuery，不要直接碰这个类。
class DbConnection final
{
public:
    DbConnection(net::EventLoop* loop, DbConfig config);
    ~DbConnection();

    DbConnection(const DbConnection&) = delete;
    DbConnection& operator=(const DbConnection&) = delete;

    // 开始连接。非阻塞，连上之后自动进入 Pipeline 模式。
    // 连不上会自己按退避重试。
    void Start();

    // 当前连接是否可用。库挂了的时候缓存仍能服务，缓存未命中的请求要靠它决定拒绝。
    [[nodiscard]] bool Connected() const noexcept;

    // 连接内部用：QueryAwaiter 挂起时把控制权交过来。
    void Submit(std::coroutine_handle<> continuation, std::string sql, std::vector<std::string> params, QueryAwaiter* awaiter);

    [[nodiscard]] net::EventLoop* Loop() const noexcept
    {
        return loop_;
    }

private:
    // 已经发出去、还没收到结果的请求。
    // 按发送顺序排队，Pipeline 保证结果回来的顺序和发送顺序一致。
    struct InFlightQuery
    {
        std::string sql;                                                                        // 失败时打日志用
        std::coroutine_handle<> continuation;
        QueryAwaiter* awaiter{nullptr};                                                         // 结果写回这个对象
    };

    // 结果已经收到、但还没等到同步点的请求。
    // 同步点之前不能交付：这一批可能是同一个隐式事务，失败要整批回滚。
    struct CompletedQuery
    {
        std::string sql;
        std::coroutine_handle<> continuation;
        QueryAwaiter* awaiter{nullptr};
        PgResult result{nullptr, &PQclear};
        std::string error;                                                                      // 空表示成功
    };

    // 这条连接现在处在哪个阶段。
    // 只有 kReady 允许发查询，其他阶段进来的查询直接报「数据库不可用」并恢复协程。
    enum class State
    {
        kIdle,        // 还没开始连
        kConnecting,  // 正在 poll 建连，还没切进 Pipeline 模式
        kReady,       // 连上了，可以收发
        kDown,        // 断了，等着退避重连
    };

    void BeginConnect();                                                                        // 调 PQconnectStart，进入轮询
    void PollConnect();                                                                         // 调 PQconnectPoll，按返回值切读写事件
    void SyncChannel();                                                                         // PG 的 socket 可能变，变了就重建 Channel
    void AttachChannel(int fd);
    void HandleReadable();                                                                      // 收数据 → flush → 解析结果
    void HandleWritable();                                                                      // 连接阶段继续 poll，查询阶段继续 flush
    void SchedulePipelineSync();                                                                // 本轮排一次批边界
    void SendPipelineSync();                                                                    // PQpipelineSync + flush
    bool FlushOutput();                                                                         // 把缓冲里的字节刷出去
    void ProcessResults();                                                                      // 收结果，按顺序配给排队中的请求
    void CompletePipeline();                                                                    // 同步点到达：整批交付
    void HandleBroken(const std::string& reason);                                               // 连接坏了：失败所有请求、排重连
    void FailAllPending(const std::string& reason);                                             // 在途和待交付的一律报「结果未知」
    void ResumeLater(std::coroutine_handle<> continuation);                                     // 投回 EventLoop 再恢复，避免回调内重入
    void ScheduleReconnect();
    void Teardown();

    net::EventLoop* loop_;
    DbConfig config_;
    PGconn* conn_{nullptr};
    std::unique_ptr<net::Channel> channel_;

    std::deque<InFlightQuery> in_flight_;                                                       // 已发送、等结果
    std::deque<CompletedQuery> completed_;                                                      // 结果已到、等同步点

    State state_{State::kIdle};
    bool sync_scheduled_{false};                                                                // 本轮已经排过批边界
    bool sync_pending_{false};                                                                  // 有请求已发但还没被 Sync 覆盖
    bool sync_failed_{false};                                                                   // 本批里有语句失败
    std::string sync_failure_message_;                                                          // 本批第一条失败的原因
    bool reconnect_scheduled_{false};
};

} // namespace ota::storage
