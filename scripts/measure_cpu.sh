#!/usr/bin/env bash
# 测某个进程在指定窗口内的真实 CPU 占用。
#
# 不能用 ps 的 %CPU：那是「进程启动以来的平均值」，
# 压测刚起 30 秒时，前面建连的尖峰全摊在里面，看不出稳态到底用了多少核。
#
# 这里读 /proc/<pid>/stat 的 utime+stime，隔一段时间再读一次，两者相减。
#
# 用法： ./measure_cpu.sh [秒数] [进程名]

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

WINDOW="${1:-20}"
NAME="${2:-ota_server}"

PID="$(pgrep -f "$NAME" | head -1)"
if [[ -z "$PID" ]]; then
    echo "找不到 $NAME 进程"
    exit 1
fi

HZ="$(getconf CLK_TCK)"

read_cpu_ticks() {
    awk '{print $14 + $15}' "/proc/$PID/stat"
}

A="$(read_cpu_ticks)"
sleep "$WINDOW"
B="$(read_cpu_ticks)"

# ticks 转成秒：除以 HZ；再除以窗口长度，得到「用了几个核」
CORES="$(awk -v a="$A" -v b="$B" -v hz="$HZ" -v w="$WINDOW" 'BEGIN { printf "%.2f", (b - a) / hz / w }')"
PERCENT="$(awk -v c="$CORES" 'BEGIN { printf "%.1f", c * 100 }')"

echo "进程        : $NAME (PID $PID)"
echo "窗口        : ${WINDOW} 秒"
echo "占用        : ${CORES} 个核  =  ${PERCENT} %"
echo "说明        : 这是窗口内的真实占用，不是启动以来的平均值"