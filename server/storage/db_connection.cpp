#include "server/storage/db_connection.h"

#include "server/storage/db_awaiter.h"

#include "nebula/base/logger.h"
#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"

#include <chrono>
#include <utility>

namespace ota::storage
{

namespace
{

// 重连退避从 2 秒起步。库重启期间不要去轰它。
constexpr std::uint32_t kReconnectMs = 2000;

// 把配置拼成 libpq 认识的连接串。
std::string MakeConnInfo(const DbConfig& config)
{
    std::string info;
    info += "host=" + config.host + " ";
    info += "port=" + std::to_string(config.port) + " ";
    info += "user=" + config.user + " ";
    info += "password=" + config.password + " ";
    info += "dbname=" + config.database + " ";
    info += "connect_timeout=5";
    return info;
}

// libpq 的错误信息末尾通常带换行，拼进日志前先去掉。
std::string TrimTrailingNewlines(std::string text)
{
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r'))
    {
        text.pop_back();
    }

    return text;
}

// 优先读结果自带的错误信息；结果本身没说明时退回连接级错误。
std::string ResultError(PGconn* connection, PGresult* result)
{
    const char* message = result == nullptr ? nullptr : PQresultErrorMessage(result);

    if (message == nullptr || message[0] == '\0')
    {
        message = connection == nullptr ? nullptr : PQerrorMessage(connection);
    }

    if (message == nullptr || message[0] == '\0')
    {
        return "unknown PostgreSQL error";
    }

    return TrimTrailingNewlines(message);
}

} // namespace

DbConnection::DbConnection(net::EventLoop* loop, DbConfig config)
    : loop_(loop)
    , config_(std::move(config))
{
}

DbConnection::~DbConnection()
{
    Teardown();
}

// 发起第一次非阻塞建连。连不上会自己退避重试。
void DbConnection::Start()
{
    BeginConnect();
}

// 连上了、并且已经切进 Pipeline 模式，才算可用。
bool DbConnection::Connected() const noexcept
{
    return state_ == State::kReady;
}

// 发一条查询：只写进 libpq 的输出缓冲，然后排一次批边界。
//
// 注意这里**不 flush**。同一轮事件处理里后面进来的请求会接着 PQsend，
// 等 QueueInLoop 的回调跑起来，才一次性 PQpipelineSync + PQflush。
void DbConnection::Submit(std::coroutine_handle<> continuation, std::string sql,
                          std::vector<std::string> params, QueryAwaiter* awaiter)
{
    if (awaiter == nullptr)
    {
        return;
    }

    // 库不可用：直接报失败。不能在 await_suspend 里直接 resume，投回 loop 再恢复。
    if (state_ != State::kReady)
    {
        awaiter->SetResult(nullptr, "数据库不可用");
        ResumeLater(continuation);
        return;
    }

    // libpq 的参数字符串数组，最后一个元素必须是空指针。
    std::vector<const char*> values;
    values.reserve(params.size() + 1);
    for (const std::string& param : params)
    {
        values.push_back(param.c_str());
    }
    values.push_back(nullptr);

    const int sent = PQsendQueryParams(conn_,
                                       sql.c_str(),
                                       static_cast<int>(params.size()),
                                       nullptr,                                                 // 参数类型交给服务端推断
                                       values.data(),
                                       nullptr,                                                 // 参数长度：全是文本
                                       nullptr,                                                 // 结果格式：文本
                                       0);                                                      // 非二进制结果

    if (sent == 0)
    {
        const std::string error = TrimTrailingNewlines(PQerrorMessage(conn_));
        NLOG_WARN("PQsendQueryParams 失败: {} | {}", sql, error);

        awaiter->SetResult(nullptr, error);
        ResumeLater(continuation);

        HandleBroken("发送查询失败: " + error);
        return;
    }

    in_flight_.push_back(InFlightQuery{std::move(sql), continuation, awaiter});

    SchedulePipelineSync();
}

// 批边界：本轮的第一个请求排一次 QueueInLoop，后面的请求看到已排队就不再排。
//
// QueueInLoop 的语义是「当前这一轮事件处理结束后再执行」，
// 所以本轮 PQsend 出去的所有查询会共用同一个同步点，而不是每条 SQL 发一次 Sync。
void DbConnection::SchedulePipelineSync()
{
    if (state_ != State::kReady || sync_scheduled_)
    {
        return;
    }

    sync_scheduled_ = true;
    sync_pending_ = true;

    loop_->QueueInLoop(
        [this]
        {
            sync_scheduled_ = false;
            SendPipelineSync();
        });
}

// 发同步点，然后把攒着的字节一次刷出去。
void DbConnection::SendPipelineSync()
{
    // PQpipelineSync 发的是 PostgreSQL 协议里的 Sync 消息（一个 'S' 字节开头的小包）。
    // 它有三个作用：
    //   · 标记这一批 SQL 的边界，服务端收到它才结束当前隐式事务
    //   · 让服务端把缓冲着的结果一起吐回来，不然它可能攒着不发
    //   · 出错时作为恢复点，错误之后到下一个 Sync 之间的语句全部跳过
    //
    // 此刻发送缓冲里的顺序：
    //
    //   PQsendQueryParams × N        这一轮发出去的 N 条 SQL，都还躺在缓冲里
    //   PQpipelineSync              ← 现在补上这一个 Sync
    //   PQflush                     一次 write 系统调用全写出去

    if (state_ != State::kReady || !sync_pending_)
    {
        return;
    }

    sync_pending_ = false;

    if (PQpipelineSync(conn_) != 1)
    {
        HandleBroken("PQpipelineSync 失败: " + TrimTrailingNewlines(PQerrorMessage(conn_)));
        return;
    }

    FlushOutput();
}

// 把 libpq 输出缓冲里的字节写到 socket。
// 返回 1 表示还有没写完的，需要等可写事件。
bool DbConnection::FlushOutput()
{
    if (conn_ == nullptr)
    {
        return false;
    }

    const int flushed = PQflush(conn_);

    if (flushed < 0)
    {
        HandleBroken("PQflush 失败: " + TrimTrailingNewlines(PQerrorMessage(conn_)));
        return false;
    }

    if (channel_ == nullptr)
    {
        return true;
    }

    if (flushed == 1)
    {
        if (!channel_->IsWriting())
        {
            channel_->EnableWriting();
        }

        return true;
    }

    if (channel_->IsWriting())
    {
        channel_->DisableWriting();
    }

    return true;
}

// 进入连接阶段。PQconnectStart 只发起，不等待。
void DbConnection::BeginConnect()
{
    reconnect_scheduled_ = false;
    Teardown();

    state_ = State::kConnecting;

    const std::string conninfo = MakeConnInfo(config_);
    conn_ = PQconnectStart(conninfo.c_str());

    if (conn_ == nullptr)
    {
        HandleBroken("PQconnectStart 返回空");
        return;
    }

    // 后面的 PQconnectPoll / PQsendQueryParams / PQflush 都依赖非阻塞模式。
    if (PQsetnonblocking(conn_, 1) != 0)
    {
        HandleBroken("PQsetnonblocking 失败");
        return;
    }

    PollConnect();
}

// 连接阶段由 PQconnectPoll 驱动：它告诉我们要等可读还是可写。
void DbConnection::PollConnect()
{
    if (conn_ == nullptr)
    {
        return;
    }

    const PostgresPollingStatusType status = PQconnectPoll(conn_);

    // PQconnectPoll 每调用一次只推进一小步，返回值告诉下一次要等什么事件。
    // 建连要来回好几轮：TCP 握手 → 发 startup 包 → 收认证请求 → 发密码 → 收认证结果 → 连上。
    switch (status)
    {
    // 下一步要等对端发数据过来（认证请求、认证结果都是这个阶段来的）
    case PGRES_POLLING_READING:
        SyncChannel();
        if (channel_ != nullptr)
        {
            channel_->EnableReading();
            channel_->DisableWriting();                            // 上一轮可能开了写事件，这里要关掉，否则会空转
        }
        return;

    // 下一步要等 socket 可写，把 startup 包或者密码发出去；读事件也留着，服务端可能同时推数据
    case PGRES_POLLING_WRITING:
        SyncChannel();
        if (channel_ != nullptr)
        {
            channel_->EnableReading();
            channel_->EnableWriting();
        }
        return;

    // 建连完成：切进 Pipeline 模式，之后只允许用异步发送接口
    case PGRES_POLLING_OK:
    {
        if (PQenterPipelineMode(conn_) != 1)
        {
            HandleBroken("PQenterPipelineMode 失败: " + TrimTrailingNewlines(PQerrorMessage(conn_)));
            return;
        }

        state_ = State::kReady;
        NLOG_INFO("PostgreSQL 已连接, database={}", config_.database);

        SyncChannel();
        if (channel_ != nullptr)
        {
            channel_->EnableReading();
            channel_->DisableWriting();                            // 建连阶段的写事件已经没用了
        }

        return;
    }

    // libpq 里 ACTIVE 已经废弃不会返回，和失败归到一起
    case PGRES_POLLING_ACTIVE:
    case PGRES_POLLING_FAILED:
    default:
        HandleBroken("连接失败: " + TrimTrailingNewlines(PQerrorMessage(conn_)));
        return;
    }
}

// PostgreSQL 的 socket 在连接阶段可能换，每次 poll 之后核对一次。
void DbConnection::SyncChannel()
{
    const int fd = PQsocket(conn_);
    if (fd < 0)
    {
        return;
    }

    if (channel_ != nullptr && channel_->Fd() == fd)
    {
        return;
    }

    AttachChannel(fd);
}

// 把 PostgreSQL 的 socket 挂进 epoll，四个回调都指向本连接的处理函数。
void DbConnection::AttachChannel(int fd)
{
    if (channel_ != nullptr)
    {
        channel_->DisableAll();
        channel_->Remove();
        channel_.reset();
    }

    channel_ = std::make_unique<net::Channel>(loop_, fd);

    channel_->SetReadCallback(
        [this]
        {
            HandleReadable();
        });

    channel_->SetWriteCallback(
        [this]
        {
            HandleWritable();
        });

    // epoll 报 ERR/HUP 时走这里，统一当连接坏了处理。
    channel_->SetCloseCallback(
        [this]
        {
            HandleBroken("socket 关闭");
        });

    channel_->SetErrorCallback(
        [this]
        {
            HandleBroken("socket 出错");
        });
}

// 可读事件的处理顺序固定：先把 socket 数据读进 libpq 缓冲，
// 再把还没发出去的数据刷出去，最后解析已经完整的结果。三步不能调换。
void DbConnection::HandleReadable()
{
    if (conn_ == nullptr)
    {
        return;
    }

    // 连接还没建好时，可读事件只是 poll 的一个阶段。
    if (state_ == State::kConnecting)
    {
        PollConnect();
        return;
    }

    if (PQconsumeInput(conn_) != 1)
    {
        HandleBroken("PQconsumeInput 失败: " + TrimTrailingNewlines(PQerrorMessage(conn_)));
        return;
    }

    if (!FlushOutput())
    {
        return;
    }

    ProcessResults();
}

// 可写：连接阶段继续 poll；查询阶段把还没 flush 完的送出去。
void DbConnection::HandleWritable()
{
    if (conn_ == nullptr)
    {
        return;
    }

    if (state_ == State::kConnecting)
    {
        PollConnect();
        return;
    }

    FlushOutput();
}

// 把已经到达的结果按发送顺序取出来，见到同步点就整批交付。
// 收尾判定比较绕，看函数里那张序列图。
void DbConnection::ProcessResults()
{
    // 一轮里发出去 N 条 SQL 加 1 个 Sync，PQgetResult 会依次返回：
    //
    //   result_1                第 1 条 SQL 的结果
    //   NULL                    ← 第 1 条结束
    //   result_2
    //   NULL                    ← 第 2 条结束
    //   ...
    //   result_N
    //   NULL                    ← 第 N 条结束
    //   PGRES_PIPELINE_SYNC     ← PQpipelineSync 的回应
    //   NULL                    ← sync 自己结束
    //   NULL                    ← 队列空了再调还是 NULL，连续第二次 → 退出
    //
    // 每条 SQL 结束都会给一个 NULL，所以只判一次就会在第 1 条 SQL 结束时退出，
    // 后面 N-1 条的结果全丢。必须连续两次拿到 NULL 才算这一批读完。
    std::size_t consecutive_empty_results = 0;

    while (PQisBusy(conn_) == 0)
    {
        PgResult result(PQgetResult(conn_), &PQclear);

        if (result == nullptr)
        {
            ++consecutive_empty_results;

            if (consecutive_empty_results >= 2)
            {
                break;
            }

            continue;
        }

        consecutive_empty_results = 0;

        const ExecStatusType status = PQresultStatus(result.get());

        // 同步点：上一条 Sync 之前的那批 SQL 全部处理完，可以统一交付了。
        if (status == PGRES_PIPELINE_SYNC)
        {
            CompletePipeline();

            if (state_ != State::kReady)
            {
                return;
            }

            continue;
        }

        if (in_flight_.empty())
        {
            HandleBroken("PostgreSQL 返回了结果，但没有对应的请求");
            return;
        }

        InFlightQuery query = std::move(in_flight_.front());
        in_flight_.pop_front();

        CompletedQuery done;
        done.sql = std::move(query.sql);
        done.continuation = query.continuation;
        done.awaiter = query.awaiter;
        done.result = std::move(result);

        // 同批里前面的语句失败，这一条被跳过，实际没有执行
        if (status == PGRES_PIPELINE_ABORTED)
        {
            done.error = "同一批里前面的语句失败，这一条没有执行";

            if (!sync_failed_)
            {
                sync_failed_ = true;
                sync_failure_message_ = done.error;
            }
        }
        // 除了这两种状态，其余都是执行失败，要留下服务端给的错误信息
        else if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
        {
            done.error = ResultError(conn_, done.result.get());

            if (!sync_failed_)
            {
                sync_failed_ = true;
                sync_failure_message_ = done.error;
            }

            // 协议级坏响应说明这条连接不能再用。
            if (status == PGRES_BAD_RESPONSE)
            {
                HandleBroken("PostgreSQL 返回了无法解析的响应: " + done.error);
                return;
            }
        }

        completed_.push_back(std::move(done));
    }
}

// 同步点到达：整批结果一起交付。
//
// 一个同步点之间的 SQL 属于同一个隐式事务。只要其中一条失败，同批的写操作也会回滚，
// 所以不能把这一批里「看起来成功」的结果提前交给协程，必须在这里统一判定。
void DbConnection::CompletePipeline()
{
    // 没有待交付的结果却收到同步点，说明本地记录和连接状态已经对不上。
    if (completed_.empty())
    {
        HandleBroken("同步点到达，但没有待交付的结果");
        return;
    }

    std::vector<std::coroutine_handle<>> continuations;
    continuations.reserve(completed_.size());

    for (CompletedQuery& done : completed_)
    {
        if (sync_failed_ && done.error.empty())
        {
            done.error = "同一批里有语句失败，这一批被回滚: " + sync_failure_message_;
        }

        if (!done.error.empty())
        {
            NLOG_WARN("SQL 失败: {} | {}", done.sql, done.error);
        }

        // 所有权转进 QueryResult 的 shared_ptr，由它负责 PQclear。
        if (done.awaiter != nullptr)
        {
            done.awaiter->SetResult(done.result.release(), done.error);
        }

        continuations.push_back(done.continuation);
    }

    const std::size_t resumed = continuations.size();

    completed_.clear();
    sync_failed_ = false;
    sync_failure_message_.clear();

    NLOG_DEBUG("Pipeline 交付 {} 条结果", resumed);

    // 不能在这里直接 resume：此刻还在 PostgreSQL socket 的事件回调里，
    // 协程恢复后可能立刻提交新请求，进而改动正在被使用的队列。
    for (const std::coroutine_handle<> continuation : continuations)
    {
        ResumeLater(continuation);
    }
}

// 连接坏了：在途和待交付的一律报「结果未知」，然后排一次重连。
void DbConnection::HandleBroken(const std::string& reason)
{
    if (state_ == State::kDown)
    {
        return;
    }

    NLOG_WARN("PostgreSQL 连接断开: {}", reason);

    state_ = State::kDown;

    FailAllPending(reason);

    // 只停事件，不在这里 PQfinish：这个函数可能正被 socket 回调调用，
    // 真正的释放留给 BeginConnect 或析构里的 Teardown。
    if (channel_ != nullptr)
    {
        channel_->DisableAll();
    }

    ScheduleReconnect();
}

// 在途和待交付的请求全部失败。
//
// 报「事务结果未知」而不是普通失败：这批 SQL 可能已经执行了、也可能被回滚了，
// 上层不能按「确定没执行」来处理。
void DbConnection::FailAllPending(const std::string& reason)
{
    std::vector<std::coroutine_handle<>> continuations;
    continuations.reserve(in_flight_.size() + completed_.size());

    const std::string error = "连接在同步点之前断开，事务结果未知: " + reason;

    for (InFlightQuery& query : in_flight_)
    {
        if (query.awaiter != nullptr)
        {
            query.awaiter->SetResult(nullptr, error);
        }

        continuations.push_back(query.continuation);
    }
    in_flight_.clear();

    for (CompletedQuery& done : completed_)
    {
        if (done.awaiter != nullptr)
        {
            done.awaiter->SetResult(nullptr, error);
        }

        continuations.push_back(done.continuation);
    }
    completed_.clear();

    sync_pending_ = false;
    sync_failed_ = false;
    sync_failure_message_.clear();

    for (const std::coroutine_handle<> continuation : continuations)
    {
        ResumeLater(continuation);
    }
}

// 投回 EventLoop 再恢复，避免在事件回调里重入协程。
void DbConnection::ResumeLater(std::coroutine_handle<> continuation)
{
    if (!continuation)
    {
        return;
    }

    loop_->QueueInLoop(
        [continuation]
        {
            continuation.resume();
        });
}

// 隔一段时间再连，不要立刻重试。
void DbConnection::ScheduleReconnect()
{
    if (reconnect_scheduled_)
    {
        return;
    }

    reconnect_scheduled_ = true;

    NLOG_INFO("{} 毫秒后重连 PostgreSQL", kReconnectMs);

    loop_->RunAfter(
        std::chrono::milliseconds(kReconnectMs),
        [this]
        {
            BeginConnect();
        });
}

// 释放 Channel 和连接。
void DbConnection::Teardown()
{
    if (channel_ != nullptr)
    {
        channel_->DisableAll();
        channel_->Remove();
        channel_.reset();
    }

    if (conn_ != nullptr)
    {
        PQfinish(conn_);
        conn_ = nullptr;
    }
}

} // namespace ota::storage
