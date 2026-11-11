#include "server/gateway/device_cache.h"

#include <utility>

namespace ota::gateway
{

DeviceCache::DeviceCache(std::chrono::seconds ttl)
    : ttl_(ttl)
{
}

// 命中就把档案拷出来；过期就顺手删掉，当成没命中。
bool DeviceCache::Lookup(const std::string& sn, CachedDevice* out)
{
    const auto it = entries_.find(sn);
    if (it == entries_.end())
    {
        return false;
    }

    if (std::chrono::steady_clock::now() >= it->second.expire_at)
    {
        entries_.erase(it);
        return false;
    }

    if (out != nullptr)
    {
        *out = it->second;
    }

    return true;
}

// 认证通过后写进缓存，过期时间按 TTL 现算。
void DeviceCache::Put(const std::string& sn, std::string model, std::string group_name, std::string fw_version)
{
    CachedDevice device;
    device.model = std::move(model);
    device.group_name = std::move(group_name);
    device.fw_version = std::move(fw_version);
    device.expire_at = std::chrono::steady_clock::now() + ttl_;

    entries_[sn] = std::move(device);
}

// 只改版本号，不动过期时间：档案没变，只是设备升到了新版本。
void DeviceCache::UpdateVersion(const std::string& sn, const std::string& fw_version)
{
    const auto it = entries_.find(sn);
    if (it == entries_.end())
    {
        return;
    }

    it->second.fw_version = fw_version;
}

// 删掉一台设备的缓存，下次重连会重新查库。
void DeviceCache::Erase(const std::string& sn)
{
    entries_.erase(sn);
}

} // namespace ota::gateway
