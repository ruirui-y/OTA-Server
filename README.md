# OTA-Server

设备集群管理服务器：10 万长连接网关、OTA 分批灰度与回滚、远程指令下发。

C++20 + NebulaRPC + PostgreSQL，配套 Qt 运维控制台与设备模拟器。

---

## 目录

| 目录 | 内容 | 编译平台 |
|---|---|---|
| `proto/` | 协议定义，服务端与客户端共用 | 全部 |
| `server/gateway/` | 设备长连接网关：收帧、认证、心跳 | Linux |
| `server/service/` | 业务服务（设备、固件、任务） | Linux |
| `server/storage/` | PostgreSQL 访问 | Linux |
| `client/` | Qt 运维控制台 | Windows |
| `simulator/` | 设备模拟器，用来压连接数 | Linux |
| `docs/` | 设计说明 | — |
| `perf-results/` | 压测原始结果 | — |

---

## 构建（Linux）

```bash
cmake --preset linux-release
cmake --build --preset build-linux-release
```

## 构建（Windows，只编客户端）

```powershell
cmake --preset windows-release
cmake --build --preset build-windows-release
```

---

## 状态

第一阶段：网关骨架（连接、认证、心跳），业务服务和数据库还没接。