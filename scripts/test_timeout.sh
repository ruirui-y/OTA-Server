#!/usr/bin/env bash
# 验证超时剔除。
#
# 关键：不能直接 kill 模拟器 —— 进程死了内核会发 FIN，服务端立刻知道连接断了，
# 那测的是 TCP 断开，不是心跳超时。
#
# 这里用 kill -STOP 把模拟器冻住：socket 还开着，但一条心跳也发不出来。
# 这才是真实的「设备拔网线 / 死机」场景。
#
# 用法： ./test_timeout.sh [设备数]

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COUNT="${1:-500}"
WARMUP=20                 # 先让连接连满、心跳跑一会儿
WAIT_AFTER_FREEZE=110     # 冻住之后等多久（服务端超时 90 秒）

pkill -f ota_ 2>/dev/null || true
sleep 0.5

$PIN "$SERVER_CPUS" "$SERVER_BIN" --listen 9500 --io 4 > "$LOG_DIR/timeout_server.log" 2>&1 &
SERVER_PID=$!
sleep 1

$PIN "$SIM_CPUS" "$SIM_BIN" --host 127.0.0.1 --port 9500 --count "$COUNT" --threads 2 --interval 30 > "$LOG_DIR/timeout_sim.log" 2>&1 &
SIM_PID=$!

echo "1) 设备 $COUNT 个已启动，心跳 30 秒一次，先跑 ${WARMUP} 秒 ..."
sleep "$WARMUP"
echo "   冻住之前的在线数："
grep -E "连接=" "$LOG_DIR/timeout_server.log" | tail -1 | sed 's/^/     /'

echo ""
echo "2) kill -STOP 冻住模拟器（socket 不断，但不发心跳）..."
kill -STOP "$SIM_PID"

echo "   等 ${WAIT_AFTER_FREEZE} 秒，看服务端会不会把它们踢掉 ..."
sleep "$WAIT_AFTER_FREEZE"

echo ""
echo "3) 冻住之后的在线数："
grep -E "连接=" "$LOG_DIR/timeout_server.log" | tail -3 | sed 's/^/     /'

echo ""
echo "=== 踢人日志 ==="
grep -E "超时踢掉|本轮踢掉" "$LOG_DIR/timeout_server.log" | tail -6 | sed 's/^/  /'

kill -CONT "$SIM_PID" 2>/dev/null || true
kill "$SERVER_PID" "$SIM_PID" 2>/dev/null || true
echo ""
echo "日志：$LOG_DIR/timeout_server.log"