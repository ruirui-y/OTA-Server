#include "server/gateway/device_server.h"

#include "nebula/base/logger.h"
#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/net/tcp_server.h"

#include <chrono>
#include <string_view>
#include <utility>

namespace ota::gateway
{

namespace
{

constexpr std::size_t kLengthFieldBytes = 4;                                // 帧头：4 字节大端长度
constexpr std::uint32_t kMaxFrameBytes = 64U * 1024U;                       // 心跳和 Hello 都很小，64KB 足够
constexpr std::uint32_t kTickMillis = 1000;                                 // 分桶每秒推进一格

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

}  // namespace

// 会话被销毁时自动从桶里摘掉。这样断开连接、踢超时都不用再记着调一次 Detach。
DeviceSession::~DeviceSession()
{
    UnlinkSession(*this);
}

DeviceServer::DeviceServer(net::EventLoop* acceptor_loop, const std::string& ip, std::uint16_t port,
                           std::uint32_t heartbeat_seconds, std::uint32_t timeout_seconds)
    : server_(std::make_unique<net::TcpServer>(acceptor_loop, ip, port))
    , heartbeat_seconds_(heartbeat_seconds == 0 ? 30 : heartbeat_seconds)
    , timeout_seconds_(timeout_seconds == 0 ? 3 * heartbeat_seconds_ : timeout_seconds)
{
    server_->SetConnectionCallback(
        [this](const net::TcpConnectionPtr& conn)
        {
            OnConnection(conn);
        });

    server_->SetMessageCallback(
        [this](const net::TcpConnectionPtr& conn, net::Buffer* buffer)
        {
            OnMessage(conn, buffer);
        });

    // 单条连接最多攒 1MB 待发，超了就断，防止卡死的设备把网关拖垮。
    server_->SetMaxOutputBufferBytes(1U * 1024U * 1024U);
}

DeviceServer::~DeviceServer() = default;

// 启动监听。每个 IO loop 起来时给自己建一份会话表和一个环形分桶，再挂上每秒推进的定时器。
void DeviceServer::Start(std::size_t io_thread_count)
{
    const std::size_t thread_count = io_thread_count == 0 ? 1 : io_thread_count;

    server_->SetIoLoopInitCallback(
        [this](net::EventLoop* loop)
        {
            auto state = std::make_unique<GatewayLoopState>();

            // 一格一秒，桶数等于超时秒数：挂进「cursor + 超时」那一格，
            // 等 cursor 转到它正好过了 timeout_seconds_ 秒。
            state->wheel.resize(timeout_seconds_);

            loop->SetContext(std::move(state));

            {
                std::lock_guard<std::mutex> lock(loops_mutex_);
                io_loops_.push_back(loop);
            }

            ScheduleTick(loop);
        });

    server_->Start(thread_count);

    NLOG_INFO("网关已启动, IO 线程数={}, 心跳间隔={}s, 超时={}s, 分桶数={}",
              thread_count, heartbeat_seconds_, timeout_seconds_, timeout_seconds_);
}

// 连接建立就建会话、挂进超时分桶；断开就把会话从表里删掉，析构会自动摘桶。
void DeviceServer::OnConnection(const net::TcpConnectionPtr& conn)
{
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

    if (it->second->authenticated)
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
void DeviceServer::OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer)
{
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
            }
        }
        else
        {
            NLOG_WARN("消息解析失败, 连接={}, 载荷={} 字节", conn->Name(), body_size);
        }

        buffer->Retrieve(kLengthFieldBytes + static_cast<std::size_t>(body_size));
    }
}

// 按 oneof 里实际带的类型分发，不写业务逻辑。
void DeviceServer::HandleMessage(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Message& message)
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

// 设备第一次上来时自报身份。骨架阶段只校验 token 非空，正式版要查库。
void DeviceServer::HandleHello(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::DeviceHello& hello)
{
    DeviceSession* session = FindSession(state, conn.get());
    if (session == nullptr)
    {
        return;
    }

    ota::control::Message reply;
    ota::control::ServerHello* server_hello = reply.mutable_server_hello();
    server_hello->set_heartbeat_seconds(heartbeat_seconds_);

    if (hello.sn().empty() || hello.token().empty())
    {
        server_hello->set_accepted(false);
        server_hello->set_reason("sn 或 token 为空");

        NLOG_WARN("Hello 被拒绝, 连接={}", conn->Name());
        Send(conn, reply);

        conn->ForceClose();
        return;
    }

    session->sn = hello.sn();
    session->model = hello.model();
    session->fw_version = hello.fw_version();
    session->authenticated = true;

    online_count_.fetch_add(1);

    server_hello->set_accepted(true);
    Send(conn, reply);

    NLOG_DEBUG("设备上线, sn={}, model={}, fw={}", session->sn, session->model, session->fw_version);
}

// 心跳本身不改状态，会话的活跃时间由「重新挂桶」隐式记录。
// 这里只顺手跟上报的固件版本对齐，并把服务端时间带回去。
void DeviceServer::HandleHeartbeat(const net::TcpConnectionPtr& conn, GatewayLoopState* state, const ota::control::Heartbeat& heartbeat)
{
    DeviceSession* session = FindSession(state, conn.get());
    if (session == nullptr)
    {
        return;
    }

    if (!heartbeat.fw_version().empty())
    {
        session->fw_version = heartbeat.fw_version();
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

        NLOG_WARN("超时踢掉连接, sn={}, 认证={}", DisplaySn(*current), current->authenticated);
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

}  // namespace ota::gateway
