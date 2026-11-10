#!/usr/bin/env bash
# 量某个进程在指定窗口内用了几个核（真实占用，不是 ps 的平均值）。
# 用法： ./cpu_of.sh <pid> [秒数]

PID="$1"
WINDOW="${2:-10}"

HZ="$(getconf CLK_TCK)"
A="$(awk '{print $14 + $15}' "/proc/$PID/stat")"
sleep "$WINDOW"
B="$(awk '{print $14 + $15}' "/proc/$PID/stat")"

awk -v a="$A" -v b="$B" -v hz="$HZ" -v w="$WINDOW" 'BEGIN { printf "%.2f", (b - a) / hz / w }'