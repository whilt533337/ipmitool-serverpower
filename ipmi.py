#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, argparse, subprocess, sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import pandas as pd

# -----------------------------
# 传感器名优先级与数值匹配
# -----------------------------
NUM_W_PAT  = re.compile(r"([-+]?\d+(?:\.\d+)?)\s*(?:W|Watts?)\b", re.IGNORECASE)
NUM_PAT    = re.compile(r"^[-+]?\d+(?:\.\d+)?$")
# 高优先级：这些命中后立刻截断
HIGH_PREF  = [
    r"\bTOTAL[_\s]?POWER\b",
    r"\bTotal[_\s]?Power\b",
    r"\bSystem\s+Power\b",
    r"\bChassis\s+Power\b",
    r"\bPlatform\s+Power\b",
    r"\bNode\s+Power\b",
]
# 次高：仅叫 "Power"（不带 CPU/MEM/GPU/PSU/Power\d）
PLAIN_POWER = re.compile(r"^\s*Power\s*$", re.IGNORECASE)

# 明确排除/降权关键词（避免抓到部件功耗或电源引脚）
EXCLUDE_HARD = re.compile(r"(CPU|MEM|GPU|FAN|HDD|NVME|RAID|PSU\d|_PIN|_POUT|IIN|IOUT|VIN|VOUT|Power\d+)", re.IGNORECASE)

def name_score(name: str) -> int:
    n = (name or "").strip()
    if not n:
        return 0
    for pat in HIGH_PREF:
        if re.search(pat, n, re.IGNORECASE):
            return 100      # 最高优先级，命中即停
    if PLAIN_POWER.match(n):
        return 90           # 仅叫 "Power" 也很可信
    if EXCLUDE_HARD.search(n):
        return 20           # 降权：CPU/MEM/PSU等
    if "power" in n.lower():
        return 40           # 其它含 power 的名，作为兜底
    return 0

# -----------------------------
# 非阻塞读取（Windows/Unix 通用）
# -----------------------------
def spawn(cmd):
    creationflags = 0
    startupinfo = None
    if os.name == "nt":
        creationflags = 0x08000000  # CREATE_NO_WINDOW
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    # 二进制管道，自己解码，才能跨平台非阻塞读取
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=False, bufsize=0,  # 原始字节
        creationflags=creationflags, startupinfo=startupinfo
    )

def bytes_available(pipe):
    if pipe is None:
        return 0
    try:
        if os.name == "nt":
            # Windows: PeekNamedPipe
            import msvcrt, ctypes
            from ctypes import wintypes
            h = msvcrt.get_osfhandle(pipe.fileno())
            avail = wintypes.DWORD()
            # BOOL PeekNamedPipe(HANDLE, LPVOID, DWORD, LPDWORD, LPDWORD, LPDWORD)
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
def sdr_elist_stream(args, ip, user, pwd):
    """
    返回：(watts, status, log_dict)
    status: ok / timeout / ipmitool_error(...) / no_power_output_sdr / spawn_error / ipmitool_not_found
    """
    cmd = ["ipmitool", "-I", args.interface, "-H", ip, "-U", user, "-P", pwd,
           "-N", str(args.net_timeout), "-R", str(args.retries_ipmi),
           "sdr", "elist"]
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

    deadline = time.monotonic() + args.timeout

    try:
        while True:
            # 超时
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
                    # EOF
                    break
                bytes_read += len(chunk)
                buf += chunk
                # 按行解析
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
                    # 值提取
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
                    # 排除一堆 "Power1/CPU*_POWER/PSU*_PIN/POUT" 等
                    if sc <= 20:
                        continue

                    # 更好就更新
                    if sc > best["score"]:
                        best = {
                            "score": sc, "watts": watts, "name": name,
                            "value_str": m.group(0) if m else value_field.split()[0],
                            "line": compress_one_line(line)
                        }

                    # 高优先级命中，立即杀进程返回
                    if sc >= 90:  # HIGH_PREF=100 或 PLAIN_POWER=90
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

        # 进程自然退出
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
# 单台主机（重试 & 合并日志）
# -----------------------------
def query_host(args, item):
    ip, user, pwd = item["ip"], item["username"], item["password"]
    attempts = max(1, args.retries + 1)
    logs = []
    final_watts = None
    final_status = "unknown"
    for a in range(1, attempts + 1):
        watts, status, lg = sdr_elist_stream(args, ip, user, pwd)
        lg["attempt"] = a
        logs.append(lg)
        if status == "ok" and isinstance(watts, (int, float)):
            final_watts = watts
            final_status = "ok"
            break
        else:
            final_status = status

    # 合并日志文本
    log_txt = " | ".join(
        f"a{L.get('attempt')}:{final_status if i==len(logs)-1 else ''}{',' if i==len(logs)-1 else ''}"
        f"{L.get('duration_s','')}s,lines={L.get('lines','')}"
        f"{',match='+L.get('match_value_str','') if L.get('match_value_str') else ''}"
        f"{',err='+L.get('stderr','') if L.get('stderr') else ''}"
        for i, L in enumerate(logs)
    )

    detail_row = {
        "room": item["room"], "rack": item["rack"], "name": item["name"],
        "ip": ip, "username": user,
        "watts": round(final_watts, 1) if isinstance(final_watts, (int, float)) else "",
        "status": final_status,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        # 合并日志字段
        "attempts": attempts,
        "duration_total_s": round(sum(L.get("duration_s", 0.0) for L in logs), 3),
        "lines_scanned": logs[-1].get("lines", 0),
        "bytes_read": logs[-1].get("bytes", 0),
        "match_name": logs[-1].get("match_name", ""),
        "match_value_str": logs[-1].get("match_value_str", ""),
        "match_line": logs[-1].get("match_line", ""),
        "last_rc": logs[-1].get("rc", ""),
        "last_stderr": logs[-1].get("stderr", ""),
        "log": log_txt,
    }
    return detail_row, final_watts

# -----------------------------
# 主流程
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="ipmitool sdr elist（流式早停）并发查询，单一Excel输出（detail含日志）")
    parser.add_argument("--input-xlsx", "-i", default="power.xlsx", help="输入Excel文件（默认 power.xlsx）")
    parser.add_argument("--input-sheet", default="Sheet1", help="输入表名（默认 Sheet1）")
    parser.add_argument("--output-xlsx", "-o", default="power_report.xlsx", help="输出Excel文件（默认 power_report.xlsx）")

    parser.add_argument("--workers", type=int, default=16, help="并发线程数（默认16）")
    parser.add_argument("--timeout", type=float, default=12.0, help="单次 sdr elist 总超时秒数（默认12）")
    parser.add_argument("--retries", type=int, default=1, help="失败重试次数（默认1）")

    parser.add_argument("--interface", choices=["lanplus", "lan"], default="lanplus", help="IPMI接口（默认lanplus）")
    parser.add_argument("--net-timeout", type=int, default=2, help="ipmitool -N （默认2s）")
    parser.add_argument("--retries-ipmi", type=int, default=1, help="ipmitool -R （默认1次）")

    args = parser.parse_args()

    if not os.path.isfile(args.input_xlsx):
        print(f"输入文件不存在：{args.input_xlsx}")
        return 1

    df_in = pd.read_excel(args.input_xlsx, sheet_name=args.input_sheet, dtype=str).fillna("")
    required = ["room", "rack", "name", "ip", "username", "password"]
    miss = [c for c in required if c not in df_in.columns]
    if miss:
        print(f"输入表缺少字段：{', '.join(miss)}")
        return 2

    items = df_in[required].to_dict(orient="records")
    racks_all = sorted({it["rack"] for it in items})
    rooms_all = sorted({it["room"] for it in items})

    detail_rows = []
    rack_sum = defaultdict(float)
    room_sum = defaultdict(float)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut = {ex.submit(query_host, args, it): it for it in items}
        for f in as_completed(fut):
            it = fut[f]
            try:
                row, watts = f.result()
            except Exception as e:
                row = {
                    "room": it["room"], "rack": it["rack"], "name": it["name"],
                    "ip": it["ip"], "username": it["username"],
                    "watts": "", "status": f"exception: {e}",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "attempts": 0, "duration_total_s": 0, "lines_scanned": 0,
                    "bytes_read": 0, "match_name": "", "match_value_str": "",
                    "match_line": "", "last_rc": "", "last_stderr": "", "log": ""
                }
                watts = None
            detail_rows.append(row)
            if isinstance(watts, (int, float)):
                rack_sum[it["rack"]] += watts
                room_sum[it["room"]] += watts

    df_detail = pd.DataFrame(detail_rows).sort_values(["room", "rack", "name", "ip"])

    df_rack = pd.DataFrame({"rack": racks_all})
    df_rack["total_watts"] = [round(rack_sum.get(r, 0.0), 1) for r in racks_all]
    df_rack["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_room = pd.DataFrame({"room": rooms_all})
    df_room["total_watts"] = [round(room_sum.get(rm, 0.0), 1) for rm in rooms_all]
    df_room["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with pd.ExcelWriter(args.output_xlsx, engine="openpyxl") as w:
        df_detail.to_excel(w, index=False, sheet_name="detail")
        df_rack.to_excel(w, index=False, sheet_name="rack_summary")
        df_room.to_excel(w, index=False, sheet_name="room_summary")

    print(f"完成：{args.output_xlsx}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
