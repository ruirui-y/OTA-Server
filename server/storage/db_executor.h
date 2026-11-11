#pragma once

#include "server/storage/db_awaiter.h"

#include <array>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace ota::storage
{

class DbConnection;

// SQL 的类型。一条连接只服务一类。
enum class QueryKind
{
    kRead = 0,
    kInsert = 1,
    kUpdate = 2,
    kDelete = 3,
};

// 四条独立的 Pipeline 连接，同一个 EventLoop 里各挂一个 Channel。
// 按 SQL 类型分开：一条 UPDATE 失败不会把同批的 SELECT 一起判成回滚，
// 某条连接断了也只影响那一类。
// 服务端代价是四个 backend 进程，每个约 3~5MB，可以忽略。
class DbExecutor final
{
public:
    DbExecutor(net::EventLoop* loop, DbConfig config);
    ~DbExecutor();

    DbExecutor(const DbExecutor&) = delete;
    DbExecutor& operator=(const DbExecutor&) = delete;

    QueryAwaiter Read(std::string sql, std::vector<std::string> params);
    QueryAwaiter Insert(std::string sql, std::vector<std::string> params);
    QueryAwaiter Update(std::string sql, std::vector<std::string> params);
    QueryAwaiter Delete(std::string sql, std::vector<std::string> params);

    // 某一类连接现在可不可用。分类判断：读挂了不影响写。
    [[nodiscard]] bool Connected(QueryKind kind) const noexcept;

private:
    QueryAwaiter Submit(QueryKind kind, std::string sql, std::vector<std::string> params);

    // 把枚举转成连接数组的下标，越界就夹到最后一条，调用方不用再判。
    static std::size_t KindIndex(QueryKind kind) noexcept;

    static constexpr std::size_t kConnectionCount = 4;
    std::array<std::unique_ptr<DbConnection>, kConnectionCount> connections_;
};

} // namespace ota::storage
