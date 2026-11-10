#!/usr/bin/env bash
# 压测：依次跑 1 万、5 万、10 万连接，每个规模跑一段时间，结果写进 logs/。
#
# 用法：
#   ./run_stress.sh            默认 60 秒一轮
#   ./run_stress.sh 120        每轮 120 秒

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

DURATION="${1:-60}"
PORT=9500

echo "服务端绑核 $SERVER_CPUS / 模拟器绑核 $SIM_CPUS / 每轮 $DURATION 秒"
kill_previous
trap stop_all EXIT INT TERM

for COUNT in 10000 50000 100000; do
    echo ""
    echo "===== $COUNT 个连接 ====="

    : > "$LOG_DIR/server.log"
    $PIN "$SERVER_CPUS" "$SERVER_BIN" --listen "$PORT" --io 4 > "$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    sleep 1

    $PIN "$SIM_CPUS" "$SIM_BIN" --host 127.0.0.1 --port "$PORT" --count "$COUNT" --threads 4 > "$LOG_DIR/sim.log" 2>&1 &
    SIM_PID=$!

    # 等它连满
    sleep 10

    # 观察一段时间，把服务端统计抄一份到结果文件
    sleep "$DURATION"
    cp "$LOG_DIR/server.log" "$LOG_DIR/result_${COUNT}.log"

    echo "  最后几行统计："
    grep -E "连接=" "$LOG_DIR/result_${COUNT}.log" | tail -3 | sed 's/^/    /'

    stop_all
    sleep 2
done

echo ""
echo "原始日志在 $LOG_DIR"