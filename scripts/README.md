# scripts

Linux 上一键跑的测试脚本。

## 第一次用

```bash
chmod +x scripts/*.sh
```

## 脚本

| 脚本 | 干什么 |
|---|---|
| `run_stage1.sh [数量] [端口]` | 起服务端 + N 个模拟设备，前台滚动看在线数 |
| `run_stress.sh [秒数]` | 依次压 1 万 / 5 万 / 10 万连接，结果存 `logs/` |
| `stop_all.sh` | 停掉所有测试进程 |

## 绑核规则

8 核机器上默认各占一半，互不抢核：

```
服务端   CPU 0-3
模拟器   CPU 4-7
```

想自己指定：

```bash
SERVER_CPUS=0-5 SIM_CPUS=6-7 ./run_stage1.sh 10000
```

## 输出

- 实时日志：`logs/server.log`、`logs/sim.log`
- 压测结果：`logs/result_<连接数>.log`