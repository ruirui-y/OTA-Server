#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <unordered_map>

namespace ota::gateway
{

// 认证通过后缓存下来的设备档案。
// expire_at 由 DeviceCache 按 TTL 算好，查的时候过期就当成没命中。
struct CachedDevice
{
    std::string model;
    std::string group_name;
    std::string fw_version;
    std::chrono::steady_clock::time_point expire_at;
};

// sn → 设备档案的缓存，只由所属 IO 线程访问，不加锁。
// 设备每次重连都会发 Hello，而档案几分钟内不会变，缓存 10 分钟能省掉绝大部分查库。
// 这里不放在线状态（那是连接表），也不放 token（只记「校验通过」这个结论）。
class DeviceCache final
{
public:
    explicit DeviceCache(std::chrono::seconds ttl = std::chrono::seconds(600));

    // 查一台设备。过期条目会被顺手删掉，所以这个方法不能是 const。
    // 命中返回 true，并把档案写进 out。
    bool Lookup(const std::string& sn, CachedDevice* out);

    // 认证通过后写进来。expire_at 由这里按 TTL 算好。
    void Put(const std::string& sn, std::string model, std::string group_name, std::string fw_version);

    // 设备升级成功、上报了新版本时更新一下，不动过期时间。
    void UpdateVersion(const std::string& sn, const std::string& fw_version);

    void Erase(const std::string& sn);

    // 当前缓存了多少台。用来观察缓存命中率。
    [[nodiscard]] std::size_t Size() const noexcept
    {
        return entries_.size();
    }

private:
    std::unordered_map<std::string, CachedDevice> entries_;
    std::chrono::seconds ttl_;
};

} // namespace ota::gateway
