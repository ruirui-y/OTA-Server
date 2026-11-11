#include "server/gateway/device_server.h"

#include "nebula/base/logger.h"
#include "nebula/net/buffer.h"
#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/net/tcp_server.h"

#include <algorithm>
#include <chrono>
#include <string_view>
#include <utility>

namespace ota::gateway
{

using net::TcpConnectionPtr;                                                                    // 下面参数统一写 TcpConnectionPtr

namespace
{

constexpr std::size_t kLengthFieldBytes = 4;                                                    // 帧头：4 字节大端长度
constexpr std::uint32_t kMaxFrameBytes = 64U * 1024U;                                           // 心跳和 Hello 都很小，64KB 足够
constexpr std::uint32_t kTickMillis = 1000;                                                     // 分桶每秒推进一格
constexpr std::size_t kMaxLatencySamples = 200000;                                          // 延迟采样上限，1.6MB

// 把一条 protobuf 消息拼上 4 字节大端长度。
std::string EncodeFrame(const google::protobuf::MessageLite& message)
{
    const std::size_t body_size = message.ByteSizeLong();

    std::string output;
    output.resize(kLengthFieldBytes + body_size);

    output[0] = static_cast<char>((body_size >> 24U) & 0xFFU);
    output[1] = static_cast<char>((body_size >> 16U) & 0xFFU);
    output[2] = static_cast<char>((body_size >> 8U) & 0xFFU);
    output[3] = static_cast<char>(body_size & 0xFFU);

    message.SerializeToArray(output.data() + kLengthFieldBytes, static_cast<int>(body_size));

    return output;
}

// 把会话从它当前所在的桶里摘下来。没挂桶就什么都不做。
void UnlinkSession(DeviceSession& session)
{
    SessionBucket* bucket = session.bucket;
    if (bucket == nullptr)
    {
        return;
    }

    if (session.wheel_prev != nullptr)
    {
        session.wheel_prev->wheel_next = session.wheel_next;
    }
    else
    {
        bucket->head = session.wheel_next;
    }

    if (session.wheel_next != nullptr)
    {
        session.wheel_next->wheel_prev = session.wheel_prev;
    }
    else
    {
        bucket->tail = session.wheel_prev;
    }

    session.wheel_prev = nullptr;
    session.wheel_next = nullptr;
    session.bucket = nullptr;
}

// 把会话挂到桶尾。挂之前先摘，保证它只会出现在一个桶里。
void LinkSession(SessionBucket& bucket, DeviceSession& session)
{
    UnlinkSession(session);

    session.wheel_prev = bucket.tail;
    session.wheel_next = nullptr;

    if (bucket.tail != nullptr)
    {
        bucket.tail->wheel_next = &session;
    }
    else
    {
        bucket.head = &session;
    }

    bucket.tail = &session;
    session.bucket = &bucket;
}

} // namespace

DeviceSession::~DeviceSession()
{
    UnlinkSession(*this);
}

DeviceServer::DeviceServer(storage::DbConfig db_config,
                           std::uint32_t heartbeat_seconds,
                           std::uint32_t timeout_seconds,
                           std::uint32_t refresh_seconds)
    : db_config_(std::move(db_config))
    , heartbeat_seconds_(heartbeat_seconds == 0 ? 30 : heartbeat_seconds)
    , timeout_seconds_(timeout_seconds == 0 ? 3 * heartbeat_seconds_ : timeout_seconds)
    , refresh_seconds_(refresh_seconds == 0 ? 60 : refresh_seconds)
{
}

DeviceServer::~DeviceServer() = default;

// 给这个 loop 建好它独享的状态：会话表、环形分桶、四条数据库连接、设备缓存。
void DeviceServer::InitializeLoop(net::EventLoop* loop)
{
    loop->AssertInLoopThread();

    auto state = std::make_unique<GatewayLoopState>();

    // 一格一秒，桶数等于超时秒数：挂进「cursor + 超时」那一格，
    // 等 cursor 转到它正好过了 timeout_seconds_ 秒。
    state->wheel.resize(timeout_seconds_);

    // 四条按 SQL 类型分开的 Pipeline 连接，由这个 loop 独占。
    state->executor = std::make_unique<storage::DbExecutor>(loop, db_config_);
    state->repository = std::make_unique<storage::DeviceRepository>(state->executor.get());

    loop->SetContext(std::move(state));

    ScheduleTick(loop);
    ScheduleRefresh(loop);

    NLOG_INFO("IO 线程已初始化, 心跳={}s, 超时={}s, 刷库={}s, 分桶={}",
              heartbeat_seconds_, timeout_seconds_, refresh_seconds_, timeout_seconds_);
}

// 连接建立就建会话、挂进超时分桶；断开就把会话从表里删掉，析构会自动摘桶。
void DeviceServer::OnConnection(const TcpConnectionPtr& conn)
{
    conn->Loop()->AssertInLoopThread();

    GatewayLoopState* state = conn->Loop()->GetContext<GatewayLoopState>();
    if (state == nullptr)
    {
        return;
    }

    if (conn->Connected())
    {
        auto session = std::make_shared<DeviceSession>();
        session->conn = conn;

        state->sessions.emplace(conn.get(), session);
        AttachSession(state, *session);

        connection_count_.fetch_add(1);
        return;
    }

    const auto it = state->sessions.find(conn.get());
    if (it == state->sessions.end())
    {
        return;
    }

    if (it->second->Authenticated())
    {
        online_count_.fetch_sub(1);
    }

    NLOG_DEBUG("设备断开, sn={}", DisplaySn(*it->second));

    // erase 之后 shared_ptr 归零，DeviceSession 析构会自己从桶里摘掉。
    state->sessions.erase(it);
    connection_count_.fetch_sub(1);
}

// 收到的字节先攒在 Buffer 里，凑够一帧就切出来。
//
// +------------------+----------------------------------+
// | 长度 4 字节 大端  | Message 序列化后的 N 字节         |
// +------------------+----------------------------------+
//
// while 处理粘包，return 处理半包。
void DeviceServer::OnMessage(const TcpConnectionPtr& conn, net::Buffer* buffer)
{
    conn->Loop()->AssertInLoopThread();

    const auto received_at = std::chrono::steady_clock::now();

    std::size_t frames = 0;

    GatewayLoopState* state = conn->Loop()->GetContext<GatewayLoopState>();
    if (state == nullptr)
    {
        return;
    }

    while (buffer->ReadableBytes() >= kLengthFieldBytes)
    {
        const std::uint32_t body_size = buffer->PeekUInt32();

        if (body_size > kMaxFrameBytes)
        {
            NLOG_WARN("帧过长, 连接={}, 长度={}", conn->Name(), body_size);
            conn->ForceClose();
            return;
        }

        if (buffer->ReadableBytes() < kLengthFieldBytes + body_size)
        {
            return;
        }

        // 直接引用 Buffer 可读区，不复制出一份 string。
        const std::string_view body(buffer->Peek() + kLengthFieldBytes, body_size);

        ota::control::Message message;
        if (message.ParseFromArray(body.data(), static_cast<int>(body.size())))
        {
            HandleMessage(conn, state, message);

            // 收到任何消息都算活着：把它从旧桶摘下来、挂到最新的桶。
            // 这里不需要「取消定时器」，重新挂本身就是取消。
            if (DeviceSession* session = FindSession(state, conn.get()); session != nullptr)
            {
                AttachSession(state, *session);

                // 认证通过的设备顺手记进「这一轮要刷 last_seen」。
                // 心跳会重复命中同一个 sn，所以用 set。
                if (session->Authenticated() && !session->sn.empty())
                {
                    state->pending_last_seen.insert(session->sn);
                }
            }
        }
        else
        {
            NLOG_WARN("消息解析失败, 连接={}, 载荷={} 字节", conn->Name(), body_size);
        }

        ++frames;
        buffer->Retrieve(kLengthFieldBytes + static_cast<std::size_t>(body_size));
    }

    // 一条连接里可能一次来了好几帧，这里记的是「这一批字节处理完」的总耗时。
    const auto spent_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - received_at)
                              .count();

    {
        std::lock_guard<std::mutex> lock(latency_mutex_);
        frames_in_window_ += frames;

        if (latency_ns_.size() < kMaxLatencySamples)
        {
            latency_ns_.push_back(spent_ns);
        }
    }
}

// 按 oneof 里实际带的类型分发，不写业务逻辑。
void DeviceServer::HandleMessage(const TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Message& message)
{
    switch (message.body_case())
    {
    case ota::control::Message::kDeviceHello:
        HandleHello(conn, state, message.device_hello());
        break;

    case ota::control::Message::kHeartbeat:
        HandleHeartbeat(conn, state, message.heartbeat());
        break;

    case ota::control::Message::kCommandAck:
        NLOG_INFO("收到指令回执, command_id={}", message.command_ack().command_id());
        break;

    case ota::control::Message::kUpgradeProgress:
        NLOG_INFO("升级进度, task={}, status={}, percent={}",
                  message.upgrade_progress().task_id(),
                  message.upgrade_progress().status(),
                  message.upgrade_progress().percent());
        break;

    default:
        NLOG_WARN("无法处理的消息类型, 连接={}, 类型={}", conn->Name(), static_cast<int>(message.body_case()));
        break;
    }
}

// 设备第一次上来时自报身份。
//
// 先查内存缓存：命中就立刻回，不碰数据库。
// 未命中才发起异步查询，这期间连接挂起（不是线程挂起），结果回来再回包。
void DeviceServer::HandleHello(const TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::DeviceHello& hello)
{
    DeviceSession* session = FindSession(state, conn.get());
    if (session == nullptr)
    {
        return;
    }

    if (session->Authenticated())
    {
        NLOG_DEBUG("重复 Hello 被忽略, sn={}", session->sn);
        return;
    }

    if (session->auth == DeviceSession::AuthState::kAuthenticating)
    {
        NLOG_DEBUG("上一条认证还没回来，忽略重复 Hello, 连接={}", conn->Name());
        return;
    }

    if (hello.sn().empty() || hello.token().empty())
    {
        RejectConnection(conn, "sn 或 token 为空");
        return;
    }

    session->sn = hello.sn();
    session->model = hello.model();
    session->fw_version = hello.fw_version();
    session->auth = DeviceSession::AuthState::kAuthenticating;

    // 1. 先看缓存。设备重连时绝大多数会命中，这条路径完全不碰数据库。
    CachedDevice cached;
    if (state->cache.Lookup(session->sn, &cached))
    {
        storage::AuthResult auth;
        auth.ok = true;
        auth.record.model = cached.model;
        auth.record.group_name = cached.group_name;
        auth.record.fw_version = cached.fw_version;

        CompleteAuthentication(conn, auth);
        return;
    }

    // 2. 缓存未命中，查库。库不可用就直接拒绝（安全优先，宁可连不上也不放行）。
    if (!state->executor->Connected(storage::QueryKind::kRead))
    {
        NLOG_WARN("数据库不可用，拒绝新设备认证, sn={}", session->sn);
        RejectConnection(conn, "服务端数据库不可用");
        return;
    }

    // 3. 起协程查库：DetachedTask 创建即开始执行，跑到 co_await 才挂起。
    HandleHelloAsync(conn, session->sn, hello.token());
}

// 认证的异步部分。整条流程是一条直线，没有嵌套回调。
coro::DetachedTask DeviceServer::HandleHelloAsync(net::TcpConnectionPtr conn, std::string sn, std::string token)
{
    GatewayLoopState* state = conn->Loop()->GetContext<GatewayLoopState>();
    if (state == nullptr)
    {
        co_return;
    }

    const storage::QueryResult result = co_await state->repository->AuthenticateQuery(sn, token);

    // 挂起期间连接可能已经断了：ForceClose 之后这里的 Connected() 是 false。
    if (!conn->Connected())
    {
        NLOG_DEBUG("认证结果回来时连接已经断了");
        co_return;
    }

    CompleteAuthentication(conn, storage::DeviceRepository::ParseAuth(result, sn));
}

// 认证结果落地：通过就写缓存、回 ServerHello、算在线；不通过就回原因并断开。
void DeviceServer::CompleteAuthentication(const TcpConnectionPtr& conn, const storage::AuthResult& auth)
{
    GatewayLoopState* state = conn->Loop()->GetContext<GatewayLoopState>();
    if (state == nullptr)
    {
        return;
    }

    DeviceSession* session = FindSession(state, conn.get());
    if (session == nullptr)
    {
        return;
    }

    if (!auth.ok)
    {
        auth_failed_count_.fetch_add(1);

        // error 非空说明是库的问题，空说明设备不存在或 token 不对。
        RejectConnection(conn, auth.error.empty() ? "认证失败" : "服务端数据库异常");
        return;
    }

    session->model = auth.record.model.empty() ? session->model : auth.record.model;
    session->fw_version = auth.record.fw_version.empty() ? session->fw_version : auth.record.fw_version;
    session->auth = DeviceSession::AuthState::kAuthenticated;

    // 认证通过的东西放进缓存，下次重连直接命中。
    state->cache.Put(session->sn, auth.record.model, auth.record.group_name, auth.record.fw_version);

    online_count_.fetch_add(1);

    ota::control::Message reply;
    ota::control::ServerHello* server_hello = reply.mutable_server_hello();
    server_hello->set_accepted(true);
    server_hello->set_heartbeat_seconds(heartbeat_seconds_);
    Send(conn, reply);

    NLOG_DEBUG("设备上线, sn={}, model={}, fw={}, 在线={}",
               session->sn, session->model, session->fw_version, online_count_.load());
}

// 拒绝一条连接：先回一个 accepted=false 的 ServerHello 说明原因，再断开。
void DeviceServer::RejectConnection(const TcpConnectionPtr& conn, const std::string& reason)
{
    if (GatewayLoopState* state = conn->Loop()->GetContext<GatewayLoopState>(); state != nullptr)
    {
        if (DeviceSession* session = FindSession(state, conn.get()); session != nullptr)
        {
            session->auth = DeviceSession::AuthState::kRejected;
        }
    }

    ota::control::Message reply;
    ota::control::ServerHello* server_hello = reply.mutable_server_hello();
    server_hello->set_accepted(false);
    server_hello->set_reason(reason);
    Send(conn, reply);

    conn->ForceClose();
}

// 心跳本身不改状态，会话的活跃时间由「重新挂桶」隐式记录。
// 这里只顺手跟上报的固件版本对齐，并把服务端时间带回去。
void DeviceServer::HandleHeartbeat(const TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Heartbeat& heartbeat)
{
    DeviceSession* session = FindSession(state, conn.get());
    if (session == nullptr)
    {
        return;
    }

    // 没认证的连接发心跳一律不理，免得绕过 Hello。
    if (!session->Authenticated())
    {
        NLOG_DEBUG("未认证的连接发心跳，忽略, 连接={}", conn->Name());
        return;
    }

    if (!heartbeat.fw_version().empty() && heartbeat.fw_version() != session->fw_version)
    {
        session->fw_version = heartbeat.fw_version();

        // 设备自己报的版本变了（比如上次升级完成随后重启），缓存和库都跟一下。
        state->cache.UpdateVersion(session->sn, session->fw_version);

        SyncVersionAsync(state, session->sn, session->fw_version);
    }

    ota::control::Message reply;
    ota::control::HeartbeatAck* ack = reply.mutable_heartbeat_ack();
    ack->set_server_time_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count());

    Send(conn, reply);
}

// 把会话挂到「现在 + 超时秒数」那一格。已经在某一格里的会被先摘下来。
// 挂和摘都只是改指针，不分配内存 —— 心跳每秒上万次也不会产生 malloc。
void DeviceServer::AttachSession(GatewayLoopState* state, DeviceSession& session)
{
    if (state->wheel.empty())
    {
        return;
    }

    const std::size_t bucket_index = (state->cursor + timeout_seconds_) % state->wheel.size();
    LinkSession(state->wheel[bucket_index], session);
}

// 每秒钟推进一格。
void DeviceServer::ScheduleTick(net::EventLoop* loop)
{
    loop->RunAfter(
        std::chrono::milliseconds(kTickMillis),
        [this, loop]
        {
            TickInLoop(loop);
            ScheduleTick(loop);
        });
}

// 指针挪到下一格，把这一格里的连接全踢掉。
// 能留到这一格，说明挂进来之后整整 timeout 秒没有过任何消息。
//
// 这里不需要临时数组：整条链先摘下来，逐个处理，处理完一个就把指针挪到下一个，
// 会话在这期间被销毁也没关系（它已经不在链上了，析构不会再摘一次）。
void DeviceServer::TickInLoop(net::EventLoop* loop)
{
    loop->AssertInLoopThread();

    GatewayLoopState* state = loop->GetContext<GatewayLoopState>();
    if (state == nullptr || state->wheel.empty())
    {
        return;
    }

    state->cursor = (state->cursor + 1) % state->wheel.size();

    SessionBucket& bucket = state->wheel[state->cursor];
    if (bucket.head == nullptr)
    {
        return;
    }

    DeviceSession* current = bucket.head;
    bucket.head = nullptr;
    bucket.tail = nullptr;

    std::size_t kicked = 0;

    while (current != nullptr)
    {
        DeviceSession* next = current->wheel_next;
        net::TcpConnectionPtr conn = current->conn;

        // 已经从链上摘下来了，清干净标记，避免析构时重复摘。
        current->wheel_prev = nullptr;
        current->wheel_next = nullptr;
        current->bucket = nullptr;

        NLOG_WARN("超时踢掉连接, sn={}, 认证={}", DisplaySn(*current), current->auth == DeviceSession::AuthState::kAuthenticated);
        ++kicked;

        // ForceClose 会触发断开回调，回调里 erase 会话，current 可能在这里被销毁，
        // 所以 next 必须提前取出来。
        if (conn)
        {
            conn->ForceClose();
        }

        current = next;
    }

    NLOG_INFO("本轮踢掉 {} 条超时连接", kicked);
}

// 每 refresh_seconds_ 秒刷一次 last_seen_at。
void DeviceServer::ScheduleRefresh(net::EventLoop* loop)
{
    loop->RunAfter(
        std::chrono::milliseconds(static_cast<std::int64_t>(refresh_seconds_) * 1000),
        [this, loop]
        {
            RefreshInLoop(loop);
            ScheduleRefresh(loop);
        });
}

// 把这一轮收集到的 sn 交给数据库，一条 UPDATE 打完，然后清空。
// 失败只记日志：下一轮还会刷，不影响任何连接。
void DeviceServer::RefreshInLoop(net::EventLoop* loop)
{
    loop->AssertInLoopThread();

    GatewayLoopState* state = loop->GetContext<GatewayLoopState>();
    if (state == nullptr || state->pending_last_seen.empty())
    {
        return;
    }

    std::vector<std::string> sns;
    sns.reserve(state->pending_last_seen.size());

    for (const std::string& sn : state->pending_last_seen)
    {
        sns.push_back(sn);
    }

    state->pending_last_seen.clear();

    if (!state->executor->Connected(storage::QueryKind::kUpdate))
    {
        NLOG_WARN("数据库不可用，跳过本轮 last_seen 刷新, {} 条", sns.size());
        return;
    }

    RefreshLastSeenAsync(state, std::move(sns));
}

// 批量刷 last_seen 的异步部分。失败只影响这一轮，下一轮还会刷。
coro::DetachedTask DeviceServer::RefreshLastSeenAsync(GatewayLoopState* state, std::vector<std::string> sns)
{
    if (state == nullptr || state->repository == nullptr)
    {
        co_return;
    }

    const storage::QueryResult result = co_await state->repository->RefreshLastSeenQuery(sns);
    storage::DeviceRepository::ParseRefresh(result, sns.size());
}

// 同步设备版本的异步部分。设备上报的版本变了才走这里，频率很低。
coro::DetachedTask DeviceServer::SyncVersionAsync(GatewayLoopState* state, std::string sn, std::string fw_version)
{
    if (state == nullptr || state->repository == nullptr)
    {
        co_return;
    }

    const storage::QueryResult result = co_await state->repository->UpdateVersionQuery(sn, fw_version);
    storage::DeviceRepository::ParseUpdateVersion(result, sn);
}

// 加长度头、序列化、写出去。
void DeviceServer::Send(const net::TcpConnectionPtr& conn, ota::control::Message& message)
{
    if (!conn || !conn->Connected())
    {
        return;
    }

    conn->Write(EncodeFrame(message));
    conn->Flush();
}

// 按连接指针找到它在本 loop 会话表里的位置；找不到说明连接刚断。
DeviceSession* DeviceServer::FindSession(GatewayLoopState* state, const net::TcpConnection* conn)
{
    const auto it = state->sessions.find(conn);
    return it == state->sessions.end() ? nullptr : it->second.get();
}

// 日志里显示用的 sn，没认证过就显示一个横杠。
std::string DeviceServer::DisplaySn(const DeviceSession& session)
{
    return session.sn.empty() ? std::string("-") : session.sn;
}

// 已认证的设备数。
std::size_t DeviceServer::OnlineCount() const
{
    return online_count_.load();
}

// 当前 TCP 连接数。
std::size_t DeviceServer::ConnectionCount() const
{
    return connection_count_.load();
}

// 打印这一轮的消息处理耗时，然后清空样本。
// 单位是纳秒，输出时换成微秒，方便和网络延迟对照。
void DeviceServer::ReportLatency()
{
    std::vector<std::int64_t> samples;
    std::size_t frames = 0;
    {
        std::lock_guard<std::mutex> lock(latency_mutex_);
        samples.swap(latency_ns_);
        frames = frames_in_window_;
        frames_in_window_ = 0;
    }

    const auto now = std::chrono::steady_clock::now();
    const double seconds = std::chrono::duration<double>(now - last_report_).count();
    last_report_ = now;

    if (samples.empty() || seconds <= 0.0)
    {
        return;
    }

    std::sort(samples.begin(), samples.end());

    const std::size_t used = samples.size();
    const auto pick = [&samples, used](double ratio)
    {
        const std::size_t index = static_cast<std::size_t>(ratio * static_cast<double>(used - 1));
        return samples[index] / 1000;
    };

    const double frame_qps = static_cast<double>(frames) / seconds;
    const double call_qps = static_cast<double>(used) / seconds;

    NLOG_INFO("QPS={:.0f} 帧/秒 ({} 次调用/秒) 样本={} p50={}us p90={}us p99={}us p999={}us max={}us",
              frame_qps,
              static_cast<std::size_t>(call_qps),
              used,
              pick(0.50), pick(0.90), pick(0.99), pick(0.999), samples[used - 1] / 1000);
}

} // namespace ota::gateway
