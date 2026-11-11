// 设备模拟器：模拟 N 个设备连上网关，发 Hello、按间隔发心跳。
//
// 用法：
//     ./ota_simulator --host 127.0.0.1 --port 9500 --count 50000 --threads 4
//
// 每个线程一个 EventLoop，各管自己那批设备。单线程在几万连接时会被 CPU 打满，
// 那时候压不上去的是模拟器，不是服务端 —— 所以必须多线程。
//
// 压测时不要和服务端抢 CPU：服务端放虚拟机，模拟器放物理机，或者各绑一半核。

#include "device_control.pb.h"

#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/event_loop_thread.h"
#include "nebula/net/tcp_client.h"
#include "nebula/net/tcp_connection.h"

#include "nebula/base/logger.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace
{

namespace net = nebula::net;

constexpr std::size_t kLengthFieldBytes = 4;                                // 帧头：4 字节大端长度
constexpr std::uint32_t kMaxFrameBytes = 64U * 1024U;                       // 单帧上限
constexpr int kBatchSize = 500;                                             // 每批创建多少个客户端

std::atomic<int> g_created{0};                                              // 已创建的客户端对象数
std::atomic<int> g_connected{0};                                            // 已建立 TCP 连接的数
std::atomic<int> g_hello_ok{0};                                             // 已通过认证的数
std::atomic<int> g_heartbeat{0};                                            // 累计发出的心跳数

// 每个模拟设备自己攒半包用的缓冲。
struct SimDevice
{
    std::string sn;
    std::string token;                                                      // 接入凭证，和库里的 token_hash 对应
    std::string pending;
    net::TcpConnectionPtr conn;                                             // 连上之后才有，发心跳要用
    std::atomic<bool> hello_ok{false};
};

// 造 sn / token：sim-000001 这种固定 6 位，和 scripts/seed_devices.sql 对得上。
std::string MakeName(const char* prefix, int index)
{
    char buffer[32] = {};
    std::snprintf(buffer, sizeof(buffer), "%s%06d", prefix, index);
    return std::string(buffer);
}

// 一个压测线程：自己的 EventLoop + 自己那批客户端。
struct Worker
{
    std::unique_ptr<net::EventLoopThread> thread;
    net::EventLoop* loop{nullptr};
    std::vector<std::shared_ptr<net::TcpClient>> clients;                   // 只在它自己的 loop 线程里写
    std::vector<std::shared_ptr<SimDevice>> devices;                        // 同上
};

// 把一条消息拼上 4 字节大端长度后写出去。
void SendFrame(const net::TcpConnectionPtr& conn, google::protobuf::MessageLite& message)
{
    if (!conn || !conn->Connected())
    {
        return;
    }

    const std::size_t body_size = message.ByteSizeLong();

    std::string output;
    output.resize(kLengthFieldBytes + body_size);
    output[0] = static_cast<char>((body_size >> 24U) & 0xFFU);
    output[1] = static_cast<char>((body_size >> 16U) & 0xFFU);
    output[2] = static_cast<char>((body_size >> 8U) & 0xFFU);
    output[3] = static_cast<char>(body_size & 0xFFU);

    message.SerializeToArray(output.data() + kLengthFieldBytes, static_cast<int>(body_size));

    conn->Write(std::move(output));
    conn->Flush();
}

// 一个线程只挂一个定时器，到点把自己名下的设备全发一遍心跳。
// 每台设备单独挂定时器的话，5 万连接就是 5 万个 timer，光建和销毁就把 CPU 吃光。
void ScheduleHeartbeat(net::EventLoop* loop, Worker* worker, std::uint32_t seconds)
{
    const std::uint32_t interval = seconds == 0 ? 30 : seconds;

    loop->RunAfter(
        std::chrono::milliseconds(static_cast<std::int64_t>(interval) * 1000),
        [loop, worker, interval]
        {
            int sent = 0;

            for (const auto& device : worker->devices)
            {
                if (!device->hello_ok.load() || !device->conn || !device->conn->Connected())
                {
                    continue;
                }

                ota::control::Message message;
                ota::control::Heartbeat* heartbeat = message.mutable_heartbeat();
                heartbeat->set_cpu_percent(20);
                heartbeat->set_mem_percent(30);
                heartbeat->set_disk_percent(40);
                heartbeat->set_fw_version("1.0.0");

                SendFrame(device->conn, message);
                ++sent;
            }

            if (sent > 0)
            {
                const int total = g_heartbeat.fetch_add(sent) + sent;
                if ((total % 50000) < sent)
                {
                    NLOG_INFO("累计发出心跳 {} 次", total);
                }
            }

            ScheduleHeartbeat(loop, worker, interval);
        });
}

// 收到服务端消息时处理：只关心「认证通过」这一件事。
void HandleIncoming(const net::TcpConnectionPtr& conn, net::Buffer* buffer, const std::shared_ptr<SimDevice>& device)
{
    device->pending.append(buffer->RetrieveAllAsString());

    while (device->pending.size() >= kLengthFieldBytes)
    {
        const auto* raw = reinterpret_cast<const unsigned char*>(device->pending.data());
        const std::uint32_t body_size = (static_cast<std::uint32_t>(raw[0]) << 24U) |
                                        (static_cast<std::uint32_t>(raw[1]) << 16U) |
                                        (static_cast<std::uint32_t>(raw[2]) << 8U) |
                                        static_cast<std::uint32_t>(raw[3]);

        if (body_size > kMaxFrameBytes)
        {
            conn->ForceClose();
            return;
        }

        if (device->pending.size() < kLengthFieldBytes + body_size)
        {
            return;
        }

        const std::string payload = device->pending.substr(kLengthFieldBytes, body_size);
        device->pending.erase(0, kLengthFieldBytes + body_size);

        ota::control::Message message;
        if (!message.ParseFromArray(payload.data(), static_cast<int>(payload.size())))
        {
            continue;
        }

        if (message.has_server_hello() && message.server_hello().accepted() && !device->hello_ok.exchange(true))
        {
            const int ok = g_hello_ok.fetch_add(1) + 1;
            if ((ok % 10000) == 0)
            {
                NLOG_INFO("已认证 {} 个设备", ok);
            }
        }
    }
}

// 在某个 worker 上分批创建它负责的那批客户端。
// 一次建太多会把「创建」和「连接」串成一条长链：前面几万条已经连上了，
// 客户端循环还没开始跑，Hello 一条都发不出去。
void StartWorker(Worker* worker, const std::string& host, std::uint16_t port, int from, int to, std::uint32_t interval_seconds)
{
    auto step = std::make_shared<std::function<void()>>();

    *step = [worker, host, port, from, to, interval_seconds, step]
    {
        const int begin = from + static_cast<int>(worker->clients.size());
        const int end = std::min(begin + kBatchSize, to);

        for (int i = begin; i < end; ++i)
        {
            auto device = std::make_shared<SimDevice>();
            device->sn = MakeName("sim-", i);
            device->token = MakeName("tok-", i);
            worker->devices.push_back(device);

            auto client = std::make_shared<net::TcpClient>(worker->loop, host, port);


            client->SetConnectionCallback(
                [device](const net::TcpConnectionPtr& conn)
                {
                    if (!conn->Connected())
                    {
                        return;
                    }

                    device->conn = conn;

                    ota::control::Message message;
                    ota::control::DeviceHello* hello = message.mutable_device_hello();
                    hello->set_sn(device->sn);
                    hello->set_model("SIM-200");
                    hello->set_fw_version("1.0.0");
                    hello->set_token(device->token);

                    SendFrame(conn, message);

                    const int connected = g_connected.fetch_add(1) + 1;
                    if ((connected % 10000) == 0)
                    {
                        NLOG_INFO("已建立连接 {} 个", connected);
                    }
                });

            client->SetMessageCallback(
                [device](const net::TcpConnectionPtr& conn, net::Buffer* buffer)
                {
                    HandleIncoming(conn, buffer, device);
                });

            client->Connect();
            worker->clients.push_back(std::move(client));
        }

        const int created = g_created.fetch_add(end - begin) + (end - begin);
        if ((created % 10000) == 0 || end == to)
        {
            NLOG_INFO("已创建 {} 个，已连上 {} 个，已认证 {} 个", created, g_connected.load(), g_hello_ok.load());
        }

        if (worker->clients.size() < static_cast<std::size_t>(to - from))
        {
            worker->loop->RunAfter(std::chrono::milliseconds(2), *step);
        }
    };

    worker->loop->RunInLoop([step] { (*step)(); });

    // 这个线程只挂一个心跳定时器，到点把它名下的设备全发一遍。
    worker->loop->RunInLoop([worker, interval_seconds]
        {
            ScheduleHeartbeat(worker->loop, worker, interval_seconds);
        });
}

} // namespace

int main(int argc, char** argv)
{
    std::string host = "127.0.0.1";
    std::uint16_t port = 9500;
    int count = 100;
    int thread_count = 4;
    std::uint32_t interval_seconds = 30;

    for (int i = 1; i < argc; ++i)
    {
        if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc)
        {
            host = argv[++i];
        }
        else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc)
        {
            port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        }
        else if (std::strcmp(argv[i], "--count") == 0 && i + 1 < argc)
        {
            count = std::atoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
        {
            thread_count = std::atoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "--interval") == 0 && i + 1 < argc)
        {
            interval_seconds = static_cast<std::uint32_t>(std::atoi(argv[++i]));
        }
    }

    if (count <= 0)
    {
        std::printf("--count 必须大于 0\n");
        return 1;
    }

    if (thread_count <= 0)
    {
        thread_count = 1;
    }

    // 设备太少时，线程数不超过设备数。
    thread_count = std::min(thread_count, count);

    NLOG_INFO("模拟 {} 个设备连 {}:{}，使用 {} 个线程", count, host, port, thread_count);

    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(static_cast<std::size_t>(thread_count));

    for (int t = 0; t < thread_count; ++t)
    {
        auto worker = std::make_unique<Worker>();
        worker->thread = std::make_unique<net::EventLoopThread>();
        worker->loop = worker->thread->StartLoop();

        // 把 count 个设备平均分给各线程，最后一批吃掉余数。
        const int from = count * t / thread_count;
        const int to = count * (t + 1) / thread_count;

        StartWorker(worker.get(), host, port, from, to, interval_seconds);

        workers.push_back(std::move(worker));
    }

    // 主线程什么都不干，让各 worker 自己跑。
    for (;;)
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        NLOG_INFO("累计 创建={} 连接={} 认证={}", g_created.load(), g_connected.load(), g_hello_ok.load());
    }
}
