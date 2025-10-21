#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ipmi.py — 读取 Excel 清单，使用 ipmitool sdr elist（流式早停）并发采集功率；
输出一个 Excel（detail + summary），其中 summary 为“机房→机柜”的分块结构；
控制台实时打印每个 IP 的开始/结束日志。

依赖：
  pip install pandas openpyxl
用法（示例）：
  python ipmi.py -i power.xlsx --sheet Sheet1 -o power_report.xlsx --workers 16 --timeout 12 --retries 1 --net-timeout 2 --retries-ipmi 1
"""

import os
import re
import sys
import time
import argparse
import subprocess
from datetime import datetime
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# -----------------------------
# 传感器名优先级与数值匹配
# -----------------------------
NUM_W_PAT  = re.compile(r"([-+]?\d+(?:\.\d+)?)\s*(?:W|Watts?)\b", re.IGNORECASE)
NUM_PAT    = re.compile(r"^[-+]?\d+(?:\.\d+)?$")

# 高优先级命名：命中即截断
HIGH_PREF  = [
    r"\bTOTAL[_\s]?POWER\b",
    r"\bTotal[_\s]?Power\b",
    r"\bTotal\s+Power\b",
    r"\bSystem\s+Power\b",
    r"\bChassis\s+Power\b",
    r"\bPlatform\s+Power\b",
    r"\bNode\s+Power\b",
]
PLAIN_POWER = re.compile(r"^\s*Power\s*$", re.IGNORECASE)

# 明确排除/降权关键词（避免抓到部件功耗或电源引脚）
EXCLUDE_HARD = re.compile(r"(CPU|MEM|GPU|FAN|HDD|NVME|RAID|PSU\d|_PIN|_POUT|IIN|IOUT|VIN|VOUT|Power\d+)", re.IGNORECASE)

def name_score(name: str) -> int:
    n = (name or "").strip()
    if not n:
        return 0
    for pat in HIGH_PREF:
        if re.search(pat, n, re.IGNORECASE):
            return 100      # 最高优先级
    if PLAIN_POWER.match(n):
        return 90           # “Power” （无后缀）可信
    if EXCLUDE_HARD.search(n):
        return 20           # CPU/PSU/引脚等，降权
    if "power" in n.lower():
        return 40           # 其它含 power 的名
    return 0

# -----------------------------
# 平台友好的子进程启动
# -----------------------------
def spawn(cmd):
    creationflags = 0
    startupinfo = None
    if os.name == "nt":
        creationflags = 0x08000000  # CREATE_NO_WINDOW
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,           # 原始字节，自己按行切
        bufsize=0,
        creationflags=creationflags,
        startupinfo=startupinfo
    )

def bytes_available(pipe):
    if pipe is None:
        return 0
    try:
        if os.name == "nt":
            import msvcrt, ctypes
            from ctypes import wintypes
            h = msvcrt.get_osfhandle(pipe.fileno())
            avail = wintypes.DWORD()
            ctypes.windll.kernel32.PeekNamedPipe(
                wintypes.HANDLE(h), None, 0, None, ctypes.byref(avail), None
            )
            return int(avail.value)
        else:
            import select
            r, _, _ = select.select([pipe], [], [], 0)
            return 65536 if r else 0
    except Exception:
        return 0

def compress_one_line(s: str, limit=800) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s[:limit]

# -----------------------------
# sdr elist：流式扫描，命中即截断；否则返回最佳匹配
# -----------------------------
def sdr_elist_stream(ipmitool_bin, interface, host, user, pwd, net_timeout, ipmi_retries, total_timeout):
    """
    返回：(watts, status, log_dict)
    status: ok / timeout / ipmitool_error(...) / no_power_output_sdr / spawn_error / ipmitool_not_found
    """
    if not ipmitool_bin:
        return None, "ipmitool_not_found", {"duration_s": 0.0}

    cmd = [
        ipmitool_bin, "-I", interface, "-H", host, "-U", user, "-P", pwd,
        "-N", str(net_timeout), "-R", str(ipmi_retries),
        "sdr", "elist"
    ]
    t0 = time.perf_counter()
    try:
        proc = spawn(cmd)
    except FileNotFoundError:
        return None, "ipmitool_not_found", {"duration_s": 0.0}
    except Exception as e:
        return None, f"spawn_error: {e}", {"duration_s": 0.0}

    out_fd = proc.stdout.fileno()
    buf = b""
    lines = 0
    bytes_read = 0

    best = {"score": -1, "watts": None, "name": "", "value_str": "", "line": ""}

    deadline = time.monotonic() + total_timeout

    try:
        while True:
            if time.monotonic() > deadline:
                try: proc.terminate()
                except Exception: pass
                try: proc.kill()
                except Exception: pass
                dur = time.perf_counter() - t0
                err = b""
                try:
                    if proc.stderr:
                        err = proc.stderr.read() or b""
                except Exception:
                    pass
                return None, "timeout", {
                    "duration_s": round(dur, 3), "lines": lines, "bytes": bytes_read,
                    "match_name": best["name"], "match_value_str": best["value_str"],
                    "match_line": best["line"], "rc": None,
                    "stderr": compress_one_line(err.decode(errors="ignore"))
                }

            avail = bytes_available(proc.stdout)
            if avail > 0:
                chunk = os.read(out_fd, min(65536, avail))
                if not chunk:
                    break
                bytes_read += len(chunk)
                buf += chunk
                while True:
                    pos = buf.find(b"\n")
                    if pos < 0:
                        break
                    raw = buf[:pos]
                    buf = buf[pos+1:]
                    line = raw.decode(errors="ignore").rstrip("\r")
                    lines += 1

                    if "|" not in line:
                        continue
                    parts = [p.strip() for p in line.split("|")]
                    name = parts[0] if len(parts) > 0 else ""
                    value_field = parts[4] if len(parts) > 4 else (parts[2] if len(parts) > 2 else "")
                    watts = None
                    m = NUM_W_PAT.search(value_field)
                    if m:
                        try: watts = float(m.group(1))
                        except ValueError: watts = None
                    if watts is None:
                        vf = value_field.split()[0] if value_field else ""
                        if NUM_PAT.match(vf):
                            try: watts = float(vf)
                            except ValueError: watts = None
                    if watts is None:
                        continue

                    sc = name_score(name)
                    if sc <= 20:
                        continue

                    if sc > best["score"]:
                        best = {
                            "score": sc, "watts": watts, "name": name,
                            "value_str": m.group(0) if m else (value_field.split()[0] if value_field else ""),
                            "line": compress_one_line(line)
                        }

                    if sc >= 90:  # 高优/Power 命中即停
                        try: proc.kill()
                        except Exception: pass
                        dur = time.perf_counter() - t0
                        return watts, "ok", {
                            "duration_s": round(dur, 3), "lines": lines, "bytes": bytes_read,
                            "match_name": name, "match_value_str": best["value_str"],
                            "match_line": best["line"], "rc": None, "stderr": ""
                        }
            else:
                if proc.poll() is not None:
                    break
                time.sleep(0.02)

        rc = proc.poll()
        dur = time.perf_counter() - t0
        if rc == 0:
            if isinstance(best["watts"], (int, float)):
                return best["watts"], "ok", {
                    "duration_s": round(dur, 3), "lines": lines, "bytes": bytes_read,
                    "match_name": best["name"], "match_value_str": best["value_str"],
                    "match_line": best["line"], "rc": rc, "stderr": ""
                }
            return None, "no_power_output_sdr", {
                "duration_s": round(dur, 3), "lines": lines, "bytes": bytes_read,
                "match_name": "", "match_value_str": "", "match_line": "", "rc": rc, "stderr": ""
            }
        else:
            err = b""
            try:
                if proc.stderr:
                    err = proc.stderr.read() or b""
            except Exception:
                pass
            return None, f"ipmitool_error(rc={rc})", {
                "duration_s": round(dur, 3), "lines": lines, "bytes": bytes_read,
                "match_name": "", "match_value_str": "", "match_line": "",
                "rc": rc, "stderr": compress_one_line(err.decode(errors="ignore"))
            }
    finally:
        try:
            if proc.stdout: proc.stdout.close()
            if proc.stderr: proc.stderr.close()
        except Exception:
            pass

# -----------------------------
# 单台主机：重试 + 合并日志 + 控制台打印
# -----------------------------
def query_one(ipmitool_bin, args, it):
    ip, user, pwd = it["ip"], it["username"], it["password"]
    tag = f"[{it['room']}/{it['rack']}] {it['name']} {ip}"
    attempts = max(1, args.retries + 1)
    all_logs = []
    final_watts = None
    final_status = "unknown"

    print(f"--> START {tag}")
    for a in range(1, attempts + 1):
        watts, status, lg = sdr_elist_stream(
            ipmitool_bin=ipmitool_bin,
            interface=args.interface,
            host=ip, user=user, pwd=pwd,
            net_timeout=args.net_timeout,
            ipmi_retries=args.retries_ipmi,
            total_timeout=args.timeout
        )
        lg["attempt"] = a
        all_logs.append(lg)

        if status == "ok" and isinstance(watts, (int, float)):
            final_watts = watts
            final_status = "ok"
            print(f"<-- DONE  {tag} | {watts:.1f} W  (attempt {a}, {lg.get('duration_s','?')}s)")
            break
        else:
            final_status = status
            print(f"!!  FAIL  {tag} | {status} (attempt {a}, {lg.get('duration_s','?')}s)")

    # 组装 detail 行（含合并日志）
    log_chunks = []
    for lg in all_logs:
        part = f"a{lg['attempt']}:{lg.get('duration_s','')}s,lines={lg.get('lines','')}"
        if lg.get("match_value_str"):
            part += f",match={lg['match_value_str']}"
        if lg.get("stderr"):
            part += f",err={lg['stderr']}"
        log_chunks.append(part)
    log_text = " | ".join(log_chunks)

    total_dur = round(sum(lg.get("duration_s", 0.0) for lg in all_logs), 3)
    last_rc = next((lg.get("rc") for lg in reversed(all_logs) if "rc" in lg), None)
    last_err = next((lg.get("stderr") for lg in reversed(all_logs) if lg.get("stderr")), "")

    detail_row = {
        "room": it["room"], "rack": it["rack"], "name": it["name"],
        "ip": ip, "username": user,
        "watts": round(final_watts, 1) if isinstance(final_watts, (int, float)) else "",
        "status": final_status,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "attempts": attempts,
        "duration_total_s": total_dur,
        "lines_scanned": all_logs[-1].get("lines", 0),
        "bytes_read": all_logs[-1].get("bytes", 0),
        "match_name": all_logs[-1].get("match_name", ""),
        "match_value_str": all_logs[-1].get("match_value_str", ""),
        "match_line": all_logs[-1].get("match_line", ""),
        "last_rc": last_rc if last_rc is not None else "",
        "last_stderr": last_err,
        "log": log_text,
    }
    return detail_row, final_watts

# -----------------------------
# Excel summary：按“机房→机柜”分块
# -----------------------------
def build_room_rack_summary(df_detail: pd.DataFrame) -> pd.DataFrame:
    """
    返回一个 DataFrame，用视觉分块的方式按“机房→机柜”列出：
      行结构大致如下：
        机房A
        rack1, total_watts
        rack2, total_watts
        小计（机房A）, sum
        （空行）
        机房B
        ...
    """
    # 只统计成功拿到 watts 的记录
    ok = df_detail.copy()
    ok = ok[pd.to_numeric(ok["watts"], errors="coerce").notna()]
    ok["watts"] = ok["watts"].astype(float)

    # 先准备所有房/柜（即使 0 也要出现）
    rooms = list(OrderedDict.fromkeys(df_detail["room"].tolist()))
    racks_by_room = {}
    for rm in rooms:
        racks = df_detail[df_detail["room"] == rm]["rack"].tolist()
        racks_by_room[rm] = list(OrderedDict.fromkeys(racks))

    rows = []
    for rm in rooms:
        # 机房标题行
        rows.append({"room": rm, "rack": "", "label": f"", "total_watts": ""})

        # 各机柜
        for rk in racks_by_room[rm]:
            w = ok[(ok["room"] == rm) & (ok["rack"] == rk)]["watts"].sum()
            rows.append({"room": rm, "rack": rk, "label": rk, "total_watts": round(float(w), 1) if w else 0.0})

        # 机房小计
        w_rm = ok[ok["room"] == rm]["watts"].sum()
        rows.append({"room": rm, "rack": "", "label": f"小计（{rm}）", "total_watts": round(float(w_rm), 1) if w_rm else 0.0})

        # 空行分隔
        rows.append({"room": "", "rack": "", "label": "", "total_watts": ""})

    df = pd.DataFrame(rows, columns=["room", "rack", "label", "total_watts"])
    return df

# -----------------------------
# 主流程
# -----------------------------
def main():
    p = argparse.ArgumentParser(description="ipmitool sdr elist（流式早停）并发功率采集；detail + room→rack summary；控制台实时日志")
    p.add_argument("-i", "--input", dest="input_xlsx", default="power.xlsx", help="输入 Excel 文件（默认 power.xlsx）")
    p.add_argument("--sheet", dest="input_sheet", default="Sheet1", help="输入表名（默认 Sheet1）")
    p.add_argument("-o", "--output", dest="output_xlsx", default="power_report.xlsx", help="输出 Excel 文件（默认 power_report.xlsx）")

    p.add_argument("--workers", type=int, default=16, help="并发线程数（默认16）")
    p.add_argument("--timeout", type=float, default=12.0, help="单次 sdr elist 总超时秒数（默认12）")
    p.add_argument("--retries", type=int, default=1, help="失败重试次数（默认1）")

    p.add_argument("--interface", choices=["lanplus", "lan"], default="lanplus", help="IPMI 接口（默认lanplus）")
    p.add_argument("--net-timeout", type=int, default=2, help="ipmitool -N（默认2）")
    p.add_argument("--retries-ipmi", type=int, default=1, help="ipmitool -R（默认1）")

    p.add_argument("--ipmitool", default="", help="ipmitool 可执行路径（留空则从 PATH 搜索）")
    args = p.parse_args()

    # 定位 ipmitool
    ipmitool_bin = args.ipmitool.strip() or "ipmitool"
    print(f"[INFO] using ipmitool: {ipmitool_bin}")

    # 读 Excel
    if not os.path.isfile(args.input_xlsx):
        print(f"[ERROR] 输入文件不存在：{args.input_xlsx}")
        return 2

    df_in = pd.read_excel(args.input_xlsx, sheet_name=args.input_sheet, dtype=str).fillna("")
    required = ["room", "rack", "name", "ip", "username", "password"]
    miss = [c for c in required if c not in df_in.columns]
    if miss:
        print(f"[ERROR] 输入表缺少字段：{', '.join(miss)}")
        return 3
    items = df_in[required].to_dict(orient="records")

    # 并发执行
    detail_rows = []
    rack_sum = defaultdict(float)  # 仅用于运行时统计进度/打印
    room_sum = defaultdict(float)

    t_start = time.time()
    print(f"[INFO] start tasks: {len(items)} hosts, workers={args.workers}, timeout={args.timeout}s, retries={args.retries}")

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_map = {ex.submit(query_one, ipmitool_bin, args, it): it for it in items}
        done_cnt = 0
        for fut in as_completed(fut_map):
            it = fut_map[fut]
            try:
                row, watts = fut.result()
            except Exception as e:
                tag = f"[{it['room']}/{it['rack']}] {it['name']} {it['ip']}"
                print(f"!!  EXC   {tag} | exception: {e}")
                row = {
                    "room": it["room"], "rack": it["rack"], "name": it["name"],
                    "ip": it["ip"], "username": it["username"],
                    "watts": "", "status": f"exception: {e}",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "attempts": 0, "duration_total_s": 0, "lines_scanned": 0, "bytes_read": 0,
                    "match_name": "", "match_value_str": "", "match_line": "",
                    "last_rc": "", "last_stderr": "", "log": ""
                }
                watts = None

            detail_rows.append(row)
            if isinstance(watts, (int, float)):
                rack_sum[it["rack"]] += watts
                room_sum[it["room"]] += watts
            done_cnt += 1
            if done_cnt % 10 == 0 or done_cnt == len(items):
                elapsed = time.time() - t_start
                print(f"[INFO] progress: {done_cnt}/{len(items)} done in {elapsed:.1f}s")

    # 生成 DataFrame
    df_detail = pd.DataFrame(detail_rows).sort_values(["room", "rack", "name", "ip"])
    df_summary = build_room_rack_summary(df_detail)

    # 写 Excel
    with pd.ExcelWriter(args.output_xlsx, engine="openpyxl") as w:
        df_detail.to_excel(w, index=False, sheet_name="detail")
        df_summary.to_excel(w, index=False, sheet_name="summary")

    print(f"[INFO] done. output: {args.output_xlsx}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
