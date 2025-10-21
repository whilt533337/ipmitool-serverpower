#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import subprocess, threading, queue, os, sys, time, shlex

APP_TITLE = "IPMI 功率采集 GUI（customtkinter 兼容版，无Spinbox）"
DEFAULT_ENGINE = "get_power_from_ipmi.py"  # 改成你的引擎脚本名也行

def to_int(s, default):
    try:
        v = int(str(s).strip())
        return v if v >= 0 else default
    except Exception:
        return default

def to_float(s, default):
    try:
        v = float(str(s).strip())
        return v if v >= 0 else default
    except Exception:
        return default

class PowerGUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.title(APP_TITLE)
        self.geometry("980x650")
        self.minsize(900, 600)

        # 状态
        self.process = None
        self.runner_thread = None
        self.stop_flag = threading.Event()
        self.log_queue = queue.Queue()
        self.heartbeat = 0
        self.running = False
        self.poll_job = None
        self.hb_job = None

        # 顶部配置区
        self.frm_top = ctk.CTkFrame(self)
        self.frm_top.pack(fill="x", padx=10, pady=10)

        # 变量
        self.var_engine = tk.StringVar(value=DEFAULT_ENGINE)
        self.var_input = tk.StringVar(value="power.xlsx")
        self.var_sheet = tk.StringVar(value="Sheet1")
        self.var_output = tk.StringVar(value="power_report.xlsx")

        self.var_workers = tk.StringVar(value="16")      # 用 Entry，字符串存储
        self.var_timeout = tk.StringVar(value="12")      # 秒（浮点也可用整数字符串表示）
        self.var_retries = tk.StringVar(value="1")
        self.var_iface = tk.StringVar(value="lanplus")
        self.var_net_timeout = tk.StringVar(value="2")
        self.var_ipmi_retries = tk.StringVar(value="1")
        self.var_keywords = tk.StringVar(value="")       # 空格或逗号分隔

        # 行1：引擎脚本
        ctk.CTkLabel(self.frm_top, text="引擎脚本").grid(row=0, column=0, padx=6, pady=6, sticky="e")
        self.ent_engine = ctk.CTkEntry(self.frm_top, textvariable=self.var_engine, width=520)
        self.ent_engine.grid(row=0, column=1, padx=6, pady=6, sticky="w")
        ctk.CTkButton(self.frm_top, text="浏览", command=self.browse_engine).grid(row=0, column=2, padx=6, pady=6)

        # 行2：输入/Sheet/输出
        ctk.CTkLabel(self.frm_top, text="输入Excel").grid(row=1, column=0, padx=6, pady=6, sticky="e")
        self.ent_input = ctk.CTkEntry(self.frm_top, textvariable=self.var_input, width=420)
        self.ent_input.grid(row=1, column=1, padx=6, pady=6, sticky="w")
        ctk.CTkButton(self.frm_top, text="选择", command=self.browse_input).grid(row=1, column=2, padx=6, pady=6)

        ctk.CTkLabel(self.frm_top, text="Sheet").grid(row=1, column=3, padx=6, pady=6, sticky="e")
        self.ent_sheet = ctk.CTkEntry(self.frm_top, textvariable=self.var_sheet, width=120)
        self.ent_sheet.grid(row=1, column=4, padx=6, pady=6, sticky="w")

        ctk.CTkLabel(self.frm_top, text="输出Excel").grid(row=1, column=5, padx=6, pady=6, sticky="e")
        self.ent_output = ctk.CTkEntry(self.frm_top, textvariable=self.var_output, width=240)
        self.ent_output.grid(row=1, column=6, padx=6, pady=6, sticky="w")
        ctk.CTkButton(self.frm_top, text="保存到…", command=self.browse_output).grid(row=1, column=7, padx=6, pady=6)

        # 行3：workers/timeout/retries/interface
        ctk.CTkLabel(self.frm_top, text="workers").grid(row=2, column=0, padx=6, pady=6, sticky="e")
        self.ent_workers = ctk.CTkEntry(self.frm_top, textvariable=self.var_workers, width=90)
        self.ent_workers.grid(row=2, column=1, padx=6, pady=6, sticky="w")

        ctk.CTkLabel(self.frm_top, text="timeout(s)").grid(row=2, column=2, padx=6, pady=6, sticky="e")
        self.ent_timeout = ctk.CTkEntry(self.frm_top, textvariable=self.var_timeout, width=90)
        self.ent_timeout.grid(row=2, column=3, padx=6, pady=6, sticky="w")

        ctk.CTkLabel(self.frm_top, text="retries").grid(row=2, column=4, padx=6, pady=6, sticky="e")
        self.ent_retries = ctk.CTkEntry(self.frm_top, textvariable=self.var_retries, width=90)
        self.ent_retries.grid(row=2, column=5, padx=6, pady=6, sticky="w")

        ctk.CTkLabel(self.frm_top, text="interface").grid(row=2, column=6, padx=6, pady=6, sticky="e")
        self.cmb_iface = ctk.CTkComboBox(self.frm_top, values=["lanplus", "lan"], variable=self.var_iface, width=100)
        self.cmb_iface.grid(row=2, column=7, padx=6, pady=6, sticky="w")

        # 行4：-N/-R/关键字
        ctk.CTkLabel(self.frm_top, text="ipmitool -N").grid(row=3, column=0, padx=6, pady=6, sticky="e")
        self.ent_net_to = ctk.CTkEntry(self.frm_top, textvariable=self.var_net_timeout, width=90)
        self.ent_net_to.grid(row=3, column=1, padx=6, pady=6, sticky="w")

        ctk.CTkLabel(self.frm_top, text="-R").grid(row=3, column=2, padx=6, pady=6, sticky="e")
        self.ent_ipmi_r = ctk.CTkEntry(self.frm_top, textvariable=self.var_ipmi_retries, width=90)
        self.ent_ipmi_r.grid(row=3, column=3, padx=6, pady=6, sticky="w")

        ctk.CTkLabel(self.frm_top, text="自定义关键字(空格/逗号分隔)").grid(row=3, column=4, padx=6, pady=6, sticky="e")
        self.ent_keywords = ctk.CTkEntry(self.frm_top, textvariable=self.var_keywords, width=320)
        self.ent_keywords.grid(row=3, column=5, columnspan=3, padx=6, pady=6, sticky="w")

        # 行5：按钮/状态/进度
        self.btn_run = ctk.CTkButton(self.frm_top, text="运行", command=self.on_run, fg_color="#0A84FF")
        self.btn_run.grid(row=4, column=0, padx=6, pady=(10,6), sticky="ew")

        self.btn_stop = ctk.CTkButton(self.frm_top, text="停止", command=self.on_stop, fg_color="#D9534F", state="disabled")
        self.btn_stop.grid(row=4, column=1, padx=6, pady=(10,6), sticky="ew")

        self.lbl_status = ctk.CTkLabel(self.frm_top, text="空闲")
        self.lbl_status.grid(row=4, column=2, columnspan=3, pady=(10,6), sticky="w")

        self.progress = ctk.CTkProgressBar(self.frm_top)
        self.progress.set(0)
        self.progress.grid(row=4, column=5, columnspan=3, padx=6, pady=(10,6), sticky="ew")

        for i in range(8):
            self.frm_top.grid_columnconfigure(i, weight=1)

        # 日志框
        self.txt_log = ctk.CTkTextbox(self, wrap="none")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=(0,10))
        self.txt_log.insert("end", f"{APP_TITLE}\n")

        # 菜单（可选）
        self._build_menu()

        # 关闭窗口时清理
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # 菜单
    def _build_menu(self):
        menu = tk.Menu(self)
        file_menu = tk.Menu(menu, tearoff=0)
        file_menu.add_command(label="打开引擎脚本…", command=self.browse_engine)
        file_menu.add_command(label="打开输入Excel…", command=self.browse_input)
        file_menu.add_command(label="另存输出Excel…", command=self.browse_output)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self.on_close)
        menu.add_cascade(label="文件", menu=file_menu)

        help_menu = tk.Menu(menu, tearoff=0)
        help_menu.add_command(label="关于", command=lambda: messagebox.showinfo("关于", APP_TITLE))
        menu.add_cascade(label="帮助", menu=help_menu)
        self.config(menu=menu)

    # 浏览
    def browse_engine(self):
        path = filedialog.askopenfilename(title="选择引擎脚本", filetypes=[("Python", "*.py"), ("All", "*.*")])
        if path:
            self.var_engine.set(path)

    def browse_input(self):
        path = filedialog.askopenfilename(title="选择输入Excel", filetypes=[("Excel", "*.xlsx"), ("All", "*.*")])
        if path:
            self.var_input.set(path)

    def browse_output(self):
        path = filedialog.asksaveasfilename(title="保存输出Excel", defaultextension=".xlsx",
                                            filetypes=[("Excel", "*.xlsx")])
        if path:
            self.var_output.set(path)

    # 构建命令
    def build_cmd(self):
        # 读取并校验数字参数（给默认值，避免空或非法）
        workers = max(1, to_int(self.var_workers.get(), 16))
        timeout_s = max(1, to_float(self.var_timeout.get(), 12.0))
        retries = max(0, to_int(self.var_retries.get(), 1))
        net_to = max(0, to_int(self.var_net_timeout.get(), 2))
        ipmi_r = max(0, to_int(self.var_ipmi_retries.get(), 1))

        # 将纠正后的值写回界面
        self.var_workers.set(str(workers))
        self.var_timeout.set(str(timeout_s))
        self.var_retries.set(str(retries))
        self.var_net_timeout.set(str(net_to))
        self.var_ipmi_retries.set(str(ipmi_r))

        py = sys.executable or "python"
        engine = self.var_engine.get().strip() or DEFAULT_ENGINE
        engine_path = os.path.abspath(engine)

        cmd = [
            py, engine_path,
            "-i", self.var_input.get().strip(),
            "--input-sheet", self.var_sheet.get().strip(),
            "-o", self.var_output.get().strip(),
            "--workers", str(workers),
            "--timeout", str(timeout_s),
            "--retries", str(retries),
            "--interface", self.var_iface.get().strip(),
            "--net-timeout", str(net_to),
            "--retries-ipmi", str(ipmi_r),
        ]
        kws = self.var_keywords.get().strip()
        if kws:
            tmp = kws.replace(",", " ")
            parts = [p for p in shlex.split(tmp) if p.strip()]
            if parts:
                cmd += ["--name-keywords"] + parts
        return cmd

    # 运行/停止
    def on_run(self):
        if self.running:
            return
        engine = self.var_engine.get().strip()
        if not engine or not os.path.exists(engine):
            messagebox.showerror("错误", "找不到引擎脚本（请检查路径）")
            return
        if not self.var_input.get().strip():
            messagebox.showerror("错误", "请输入输入Excel路径")
            return

        cmd = self.build_cmd()
        self._toggle_controls(disabled=True)
        self.running = True
        self.stop_flag.clear()
        self.lbl_status.configure(text="运行中…")
        self._append_log(f"[Running] {' '.join(shlex.quote(c) for c in cmd)}\n")

        self.runner_thread = threading.Thread(target=self._run_subprocess, args=(cmd,), daemon=True)
        self.runner_thread.start()
        self._start_poll()
        self._start_heartbeat()

    def on_stop(self):
        if not self.running:
            return
        self.stop_flag.set()
        self._append_log("[Stop Requested] 正在尝试停止任务…\n")

    def on_close(self):
        if self.running:
            if messagebox.askyesno("确认", "任务仍在运行，确定要退出吗？"):
                self.stop_flag.set()
                self.after(300, self.destroy)
        else:
            self.destroy()

    # 子进程执行
    def _run_subprocess(self, cmd):
        creationflags = 0
        startupinfo = None
        if os.name == "nt":
            creationflags = 0x08000000  # CREATE_NO_WINDOW
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        try:
            self.process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, universal_newlines=True,
                creationflags=creationflags, startupinfo=startupinfo
            )
        except Exception as e:
            self.log_queue.put(f"[LauncherError] {e}\n")
            self._finish_run()
            return

        try:
            while True:
                if self.stop_flag.is_set():
                    try: self.process.terminate()
                    except Exception: pass
                    try: self.process.kill()
                    except Exception: pass
                    self.log_queue.put("[Stopped] 用户中止运行\n")
                    break
                line = self.process.stdout.readline()
                if not line:
                    if self.process.poll() is not None:
                        break
                    time.sleep(0.02)
                    continue
                self.log_queue.put(line)
            rc = self.process.poll()
            self.log_queue.put(f"[ExitCode] {rc}\n")
        finally:
            self._finish_run()

    def _finish_run(self):
        self.running = False
        self._toggle_controls(disabled=False)
        self.lbl_status.configure(text="空闲")
        if self.poll_job:
            try: self.after_cancel(self.poll_job)
            except Exception: pass
            self.poll_job = None
        if self.hb_job:
            try: self.after_cancel(self.hb_job)
            except Exception: pass
            self.hb_job = None
        self.progress.set(0)

    # 轮询日志/心跳
    def _start_poll(self):
        def poll():
            try:
                while True:
                    line = self.log_queue.get_nowait()
                    self._append_log(line)
            except queue.Empty:
                pass
            if self.running:
                self.poll_job = self.after(100, poll)
        self.poll_job = self.after(100, poll)

    def _start_heartbeat(self):
        def hb():
            if self.running:
                self.heartbeat = (self.heartbeat + 5) % 100
                self.progress.set(self.heartbeat / 100.0)
                self.hb_job = self.after(150, hb)
        self.hb_job = self.after(150, hb)

    # 工具
    def _append_log(self, text):
        self.txt_log.insert("end", text)
        self.txt_log.see("end")

    def _toggle_controls(self, disabled: bool):
        state = "disabled" if disabled else "normal"
        widgets = [
            self.ent_engine, self.ent_input, self.ent_sheet, self.ent_output,
            self.ent_workers, self.ent_timeout, self.ent_retries,
            self.cmb_iface, self.ent_net_to, self.ent_ipmi_r, self.ent_keywords,
        ]
        for w in widgets:
            try:
                w.configure(state=state)
            except Exception:
                pass
        self.btn_run.configure(state=("disabled" if disabled else "normal"))
        self.btn_stop.configure(state=("normal" if disabled else "disabled"))

def main():
    app = PowerGUI()
    app.mainloop()

if __name__ == "__main__":
    main()
