#!/usr/bin/env bash
# 第一阶段验证：起服务端 + 起 N 个模拟设备，看在线数稳不稳。
#
# 用法：
#   ./run_stage1.sh                  1000 个设备，跑 60 秒后自动汇总退出
#   ./run_stage1.sh 10000            一万个
#   ./run_stage1.sh 10000 9500 120   一万个，端口 9500，跑 120 秒
#   ./run_stage1.sh 1000  9500  0    时长给 0 就一直跑，Ctrl+C 停

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COUNT="${1:-1000}"
PORT="${2:-9500}"
DURATION="${3:-60}"

echo "机器核数    : $TOTAL_CPU"
echo "服务端绑核  : $SERVER_CPUS"
echo "模拟器绑核  : $SIM_CPUS"
echo "设备数量    : $COUNT"
echo "端口        : $PORT"
echo "心跳间隔    : ${INTERVAL:-30} 秒"
echo "观察时长    : $([[ "$DURATION" == "0" ]] && echo "不限，Ctrl+C 停" || echo "${DURATION} 秒")"
echo "日志        : $LOG_DIR"
echo ""

kill_previous
trap stop_all EXIT INT TERM

# 服务端：4 个 IO 线程
$PIN "$SERVER_CPUS" "$SERVER_BIN" --listen "$PORT" --io 4 > "$LOG_DIR/server.log" 2>&1 &
SERVER_PID=$!
sleep 1

# 模拟器：所有连接跑在一个 EventLoop 里，省线程
$PIN "$SIM_CPUS" "$SIM_BIN" --host 127.0.0.1 --port "$PORT" --count "$COUNT" --threads 4 --interval "${INTERVAL:-30}" > "$LOG_DIR/sim.log" 2>&1 &
SIM_PID=$!

echo "服务端 PID=$SERVER_PID  模拟器 PID=$SIM_PID"
echo "------------------------------------------------------"

# 时长给 0 就一直跟日志
if [[ "$DURATION" == "0" ]]; then
    tail -f "$LOG_DIR/server.log"
    exit 0
fi

tail -f "$LOG_DIR/server.log" &
TAIL_PID=$!
sleep "$DURATION"
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true

echo ""
echo "================ 汇总 ================"
grep -E "连接=" "$LOG_DIR/server.log" | tail -6 | sed 's/^/  /'
echo "--------------------------------------"
echo "  设备数量  : $COUNT"
echo "  观察时长  : ${DURATION} 秒"
echo "  日志文件  : $LOG_DIR/server.log"
echo "======================================"