#!/usr/bin/env bash
# 阶梯压测：逐级放大连接数，输出每个规模的稳态数据。
#
# 用法： ./bench.sh [心跳间隔秒] [每个规模测量窗口秒]
#
# 每一级：起服务端 → 起模拟器 → 预热 → 窗口内量 CPU → 记在线数 → 收工

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

INTERVAL="${1:-30}"
WINDOW="${2:-15}"
WARMUP=25
PORT=9500
SCALES=(${SCALES:-1000 5000 10000 20000})

echo "心跳间隔 ${INTERVAL}s / 预热 ${WARMUP}s / 测量窗口 ${WINDOW}s"
echo "服务端绑核 $SERVER_CPUS，模拟器绑核 $SIM_CPUS"
echo ""
printf "%-8s %-8s %-12s %-12s %-8s\n" "设备数" "在线" "服务端(核)" "模拟器(核)" "错误"
echo "----------------------------------------------------------"

for COUNT in "${SCALES[@]}"; do
    pkill -f ota_ 2>/dev/null || true
    sleep 0.5

    : > "$LOG_DIR/server.log"
    $PIN "$SERVER_CPUS" "$SERVER_BIN" --listen "$PORT" --io 4 > "$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    sleep 1

    $PIN "$SIM_CPUS" "$SIM_BIN" --host 127.0.0.1 --port "$PORT" --count "$COUNT" --threads 4 --interval "$INTERVAL" > "$LOG_DIR/sim.log" 2>&1 &
    SIM_PID=$!

    sleep "$WARMUP"

    SERVER_CORES="$("$SCRIPT_DIR/cpu_of.sh" "$SERVER_PID" "$WINDOW")"
    SIM_CORES="$("$SCRIPT_DIR/cpu_of.sh" "$SIM_PID" 5)"

    ONLINE="$(grep -oE '在线=[0-9]+' "$LOG_DIR/server.log" | tail -1 | cut -d= -f2)"
    ERRORS="$(grep -cE 'warn|error' "$LOG_DIR/server.log" || true)"

    printf "%-8s %-8s %-12s %-12s %-8s\n" "$COUNT" "$ONLINE" "$SERVER_CORES" "$SIM_CORES" "$ERRORS"
done

pkill -f ota_ 2>/dev/null || true
echo ""
echo "日志在 $LOG_DIR"