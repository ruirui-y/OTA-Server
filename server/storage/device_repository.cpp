#include "server/storage/device_repository.h"

#include "nebula/base/logger.h"

#include <cstdlib>
#include <utility>

namespace ota::storage
{

namespace
{

// 三条 SQL 放在文件作用域。
const char* const kAuthSql =
    "SELECT model, group_name, fw_version "
    "FROM devices "
    "WHERE sn = $1 AND token_hash = encode(sha256($2::bytea), 'hex')";

const char* const kRefreshSql =
    "UPDATE devices SET last_seen_at = now(), updated_at = now() "
    "WHERE sn = ANY(string_to_array($1, ','))";

const char* const kUpdateVersionSql =
    "UPDATE devices SET fw_version = $2, updated_at = now() WHERE sn = $1";

// 把若干 sn 拼成一个逗号串，交给 string_to_array 拆。
// sn 里只有字母数字和短横线，不会有逗号，但仍然检查一下，避免拼出奇怪的参数。
std::string JoinSns(const std::vector<std::string>& sns)
{
    std::string joined;
    joined.reserve(sns.size() * 12);

    for (const std::string& sn : sns)
    {
        if (sn.find(',') != std::string::npos)
        {
            continue;
        }

        if (!joined.empty())
        {
            joined.push_back(',');
        }

        joined += sn;
    }

    return joined;
}

} // namespace

DeviceRepository::DeviceRepository(DbExecutor* executor)
    : executor_(executor)
{
}

// 发认证查询。读连接。
QueryAwaiter DeviceRepository::AuthenticateQuery(const std::string& sn, const std::string& token)
{
    return executor_->Read(kAuthSql, {sn, token});
}

// 发批量刷 last_seen 的语句。写连接。
QueryAwaiter DeviceRepository::RefreshLastSeenQuery(const std::vector<std::string>& sns)
{
    // 只有一条 SQL，但参数很长：每个 sn 连逗号 11 个字符，5 万个约 537KB。
    //
    // 这个长度带来两件事：
    //   · PQflush 大概率一次写不完，返回 1 之后要等可写事件再 flush（db_connection 里已处理）
    //   · 服务端要 string_to_array 拆开，再做 5 万次索引查找
    //
    // 实测 5 万设备能跑（60 秒一次，48325 行更新成功）。到 50 万设备时这条 SQL 会到 5MB，
    // 那时要改：按 5000 个一批拆成几条 UPDATE，或者先落到临时表再 JOIN。
    return executor_->Update(kRefreshSql, {JoinSns(sns)});
}

// 发更新版本的语句。写连接。
QueryAwaiter DeviceRepository::UpdateVersionQuery(const std::string& sn, const std::string& fw_version)
{
    return executor_->Update(kUpdateVersionSql, {sn, fw_version});
}

// 解析认证结果：查不到就是认证失败，查出错就是库的问题，两种情况分开记。
AuthResult DeviceRepository::ParseAuth(const QueryResult& result, const std::string& sn)
{
    AuthResult auth;

    if (!result.Ok())
    {
        auth.error = result.Error().empty() ? std::string("查询失败") : result.Error();
        return auth;
    }

    // 设备不存在或者 token 不对。不区分这两种：区分了等于告诉对方 sn 是对的。
    if (PQntuples(result.Get()) == 0)
    {
        NLOG_WARN("认证失败, sn={}", sn);
        return auth;
    }

    auth.ok = true;
    auth.record.model = PQgetvalue(result.Get(), 0, 0);
    auth.record.group_name = PQgetvalue(result.Get(), 0, 1);
    auth.record.fw_version = PQgetvalue(result.Get(), 0, 2);

    return auth;
}

// 解析刷库结果。受影响行数从 PGcmdTuples 拿，文本形式。
std::uint64_t DeviceRepository::ParseRefresh(const QueryResult& result, std::size_t submitted)
{
    if (!result.Ok())
    {
        NLOG_WARN("批量刷 last_seen 失败: {}", result.Error());
        return 0;
    }

    const char* tuples = PQcmdTuples(result.Get());
    const std::uint64_t updated = (tuples != nullptr && tuples[0] != '\0')
                                      ? static_cast<std::uint64_t>(std::strtoull(tuples, nullptr, 10))
                                      : 0;

    NLOG_DEBUG("批量刷 last_seen 完成, 提交 {} 条, 更新 {} 行", submitted, updated);

    return updated;
}

// 解析更新版本的结果。
bool DeviceRepository::ParseUpdateVersion(const QueryResult& result, const std::string& sn)
{
    if (!result.Ok())
    {
        NLOG_WARN("同步设备版本失败, sn={}, error={}", sn, result.Error());
        return false;
    }

    return true;
}

} // namespace ota::storage
