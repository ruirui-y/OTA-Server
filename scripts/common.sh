#!/usr/bin/env bash
# 所有测试脚本共用的东西：路径、二进制、CPU 分组。
# 用法： source "$(dirname "$0")/common.sh"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

# 找可执行文件：优先 release，没有再退 debug。
find_bin() {
    local name="$1"
    for dir in "$ROOT/build/linux-release/bin" "$ROOT/build/linux-debug/bin"; do
        if [[ -x "$dir/$name" ]]; then
            echo "$dir/$name"
            return 0
        fi
    done
    echo "找不到 $name，先编译： cmake --build --preset build-linux-release" >&2
    return 1
}

SERVER_BIN="$(find_bin ota_server)"
SIM_BIN="$(find_bin ota_simulator)"

# 8 核机器上服务端和模拟器各占一半，避免互相抢核。
# 想自己指定： SERVER_CPUS=0-1 SIM_CPUS=2-3 ./run_stage1.sh
TOTAL_CPU="$(nproc)"
HALF=$(( TOTAL_CPU / 2 ))
SERVER_CPUS="${SERVER_CPUS:-0-$(( HALF - 1 ))}"
SIM_CPUS="${SIM_CPUS:-$HALF-$(( TOTAL_CPU - 1 ))}"

# 没有 taskset 就退回不绑定核心。
if command -v taskset >/dev/null 2>&1; then
    PIN="taskset -c"
else
    PIN=""
    echo "提示：没有 taskset，跳过绑核" >&2
fi

# 杀掉上一次留下的进程，避免端口被占。
kill_previous() {
    pkill -f ota_server 2>/dev/null || true
    pkill -f ota_simulator 2>/dev/null || true
    sleep 0.3
}

stop_all() {
    kill "${SERVER_PID:-}" "${SIM_PID:-}" 2>/dev/null || true
    wait "${SERVER_PID:-}" "${SIM_PID:-}" 2>/dev/null || true
}