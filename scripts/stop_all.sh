#!/usr/bin/env bash
# 把所有测试进程停掉。

pkill -f ota_server 2>/dev/null || true
pkill -f ota_simulator 2>/dev/null || true
echo "已停止 ota_server / ota_simulator"