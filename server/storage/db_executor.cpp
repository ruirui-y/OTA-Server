#include "server/storage/db_executor.h"

#include "server/storage/db_connection.h"

#include "nebula/base/logger.h"

#include <algorithm>
#include <cstddef>
#include <utility>

namespace ota::storage
{

// 把枚举转成连接数组的下标。
std::size_t DbExecutor::KindIndex(QueryKind kind) noexcept
{
    return std::min(static_cast<std::size_t>(kind), kConnectionCount - 1);
}

DbExecutor::DbExecutor(net::EventLoop* loop, DbConfig config)
{
    for (std::size_t index = 0; index < kConnectionCount; ++index)
    {
        connections_[index] = std::make_unique<DbConnection>(loop, config);
        connections_[index]->Start();
    }

    NLOG_INFO("数据库连接已建 {} 条（读/插/改/删各一条）, database={}", kConnectionCount, config.database);
}

DbExecutor::~DbExecutor() = default;

// 发一条读查询。
QueryAwaiter DbExecutor::Read(std::string sql, std::vector<std::string> params)
{
    return Submit(QueryKind::kRead, std::move(sql), std::move(params));
}

// 发一条插入。
QueryAwaiter DbExecutor::Insert(std::string sql, std::vector<std::string> params)
{
    return Submit(QueryKind::kInsert, std::move(sql), std::move(params));
}

// 发一条更新。
QueryAwaiter DbExecutor::Update(std::string sql, std::vector<std::string> params)
{
    return Submit(QueryKind::kUpdate, std::move(sql), std::move(params));
}

// 发一条删除。
QueryAwaiter DbExecutor::Delete(std::string sql, std::vector<std::string> params)
{
    return Submit(QueryKind::kDelete, std::move(sql), std::move(params));
}

// 按类型挑连接，交给 CoQuery 生成等待点。
QueryAwaiter DbExecutor::Submit(QueryKind kind, std::string sql, std::vector<std::string> params)
{
    const std::size_t index = KindIndex(kind);
    return CoQuery(*connections_[index], std::move(sql), std::move(params));
}

// 某一类连接是否可用。
bool DbExecutor::Connected(QueryKind kind) const noexcept
{
    const std::size_t index = KindIndex(kind);
    return connections_[index] != nullptr && connections_[index]->Connected();
}

} // namespace ota::storage
