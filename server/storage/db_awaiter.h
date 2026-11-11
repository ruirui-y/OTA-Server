#pragma once

#include "server/storage/db_connection.h"

#include <libpq-fe.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ota::storage
{

// PGresult 的释放器。
// 交给 shared_ptr 当 deleter，只在最后一个引用消失时调一次 PQclear。
struct PgResultDeleter
{
    void operator()(PGresult* result) const noexcept
    {
        if (result != nullptr)
        {
            PQclear(result);
        }
    }
};

// 一次查询的结果。
//
// 刻意做成「可拷贝的普通结构体」：拷贝、移动、析构全由编译器生成，
// 资源用 shared_ptr 持有，拷贝只是加个引用计数。
struct QueryResult
{
    std::shared_ptr<PGresult> result;
    std::string error;

    [[nodiscard]] bool Ok() const noexcept
    {
        return error.empty() && result != nullptr;
    }

    [[nodiscard]] PGresult* Get() const noexcept
    {
        return result.get();
    }

    [[nodiscard]] const std::string& Error() const noexcept
    {
        return error;
    }
};

// 一次查询的等待点。
// 协程里写 QueryResult result = co_await CoQuery(connection, sql, params)。
// 挂起时把 handle 交给连接，结果回来时连接调 SetResult 填好结果、再恢复协程。
class QueryAwaiter final
{
public:
    QueryAwaiter(DbConnection& connection, std::string sql, std::vector<std::string> params)
        : connection_(connection)
        , sql_(std::move(sql))
        , params_(std::move(params))
    {
    }

    QueryAwaiter(const QueryAwaiter&) = delete;
    QueryAwaiter& operator=(const QueryAwaiter&) = delete;

    [[nodiscard]] bool await_ready() const noexcept
    {
        return false;
    }

    // 挂起：把 coroutine_handle 交给连接，由它在结果回来时恢复。
    void await_suspend(std::coroutine_handle<> continuation)
    {
        connection_.Submit(continuation, std::move(sql_), std::move(params_), this);
    }

    // 结果按值交出去，和 NebulaRPC 的 RpcAwaiter::await_resume 是同一个形状。
    QueryResult await_resume() noexcept
    {
        return std::move(result_);
    }

    // 连接内部用：把结果写回这里。
    void SetResult(PGresult* result, std::string error)
    {
        result_ = QueryResult{std::shared_ptr<PGresult>(result, PgResultDeleter{}), std::move(error)};
    }

private:
    DbConnection& connection_;
    std::string sql_;
    std::vector<std::string> params_;
    QueryResult result_;
};

// 发一条查询，返回可以 co_await 的等待点。
// 对应 NebulaRPC 的 CoCall：业务代码只需要看见这一行。
[[nodiscard]] inline QueryAwaiter CoQuery(DbConnection& connection, std::string sql, std::vector<std::string> params)
{
    return QueryAwaiter(connection, std::move(sql), std::move(params));
}

} // namespace ota::storage
