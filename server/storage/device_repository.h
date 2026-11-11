#pragma once

#include "server/storage/db_executor.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace ota::storage
{

// 从库里查出来的一台设备。
// 只带认证和缓存需要的字段，不查整行。
struct DeviceRecord
{
    std::string model;
    std::string group_name;
    std::string fw_version;
};

// 认证结果。
// ok=false 且 error 为空 = 设备不存在或 token 不对（业务拒绝）；
// ok=false 且 error 非空 = 数据库出问题（要区分开，处理方式不一样）。
struct AuthResult
{
    bool ok{false};
    DeviceRecord record;
    std::string error;
};

// 设备档案的 SQL 封装：认证走读连接，刷 last_seen 和同步版本走写连接。
// 这里一个协程都没有，只提供「把查询发出去的等待点」和「解析结果的普通函数」，
// co_await 由调用方写（GCC 的协程实现在这个编译单元里会 ICE）。
class DeviceRepository final
{
public:
    explicit DeviceRepository(DbExecutor* executor);

    // 发一条认证查询，返回可以 co_await 的等待点。
    // token 在库里存的是 SHA-256，这里让数据库自己算哈希，明文 token 不落任何地方。
    QueryAwaiter AuthenticateQuery(const std::string& sn, const std::string& token);

    // 发一条批量刷 last_seen 的语句。
    QueryAwaiter RefreshLastSeenQuery(const std::vector<std::string>& sns);

    // 发一条更新版本的语句。
    QueryAwaiter UpdateVersionQuery(const std::string& sn, const std::string& fw_version);

    // 解析认证结果。
    static AuthResult ParseAuth(const QueryResult& result, const std::string& sn);

    // 解析刷库结果，返回受影响行数。
    static std::uint64_t ParseRefresh(const QueryResult& result, std::size_t submitted);

    // 解析更新版本的结果。
    static bool ParseUpdateVersion(const QueryResult& result, const std::string& sn);

private:
    DbExecutor* executor_;
};

} // namespace ota::storage
