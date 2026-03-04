# -*- coding: utf-8 -*-
"""
@Description: 基于 Tkinter 的可视化交互界面，支持多线程异步调用脱敏与还原引擎。
"""

import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
import sys
import threading
import os

try:
    from main_app_universal import DocumentProcessor, SecureDB, NER_MODEL_PATH, DB_PATH, MASTER_KEY
    from restore_universal import Restorer
except ImportError as e:
    print(f"[ERROR] Dependency missing or import failed.\n{e}")
    import time
    time.sleep(10)
    sys.exit(1)


class Logger(object):
    """标准输出重定向类：将控制台 stdout 捕获并渲染至 GUI 的 Text 控件"""
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, string):
        try:
            self.text_widget.configure(state='normal')
            self.text_widget.insert("end", string)
            self.text_widget.see("end")
            self.text_widget.configure(state='disabled')
            self.text_widget.update_idletasks()
        except:
            pass

    def flush(self):
        pass


class PrivacyApp:
    def __init__(self, root):
        self.root = root
        self.root.title("多模态文档隐私保护系统 (桌面控制台)")
        self.root.geometry("700x550")

        # 1. 配置区域
        config_frame = ttk.LabelFrame(root, text=" 目录配置 ", padding=10)
        config_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(config_frame, text="输入目录:").grid(row=0, column=0, sticky="w")
        self.input_path = tk.StringVar(value="./input_file")
        ttk.Entry(config_frame, textvariable=self.input_path, width=50).grid(row=0, column=1, padx=5)
        ttk.Button(config_frame, text="浏览", command=self.select_input).grid(row=0, column=2)

        ttk.Label(config_frame, text="输出目录:").grid(row=1, column=0, sticky="w")
        self.output_path = tk.StringVar(value="./output_file")
        ttk.Entry(config_frame, textvariable=self.output_path, width=50).grid(row=1, column=1, padx=5)
        ttk.Button(config_frame, text="浏览", command=self.select_output).grid(row=1, column=2)

        # 2. 控制区域
        action_frame = ttk.LabelFrame(root, text=" 系统操作 ", padding=10)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.btn_mask = ttk.Button(action_frame, text="执行语义脱敏 (Encode)", command=self.run_desensitize_thread)
        self.btn_mask.pack(side="left", expand=True, fill="x", padx=10, ipady=10)

        self.btn_restore = ttk.Button(action_frame, text="执行安全还原 (Decode)", command=self.run_restore_thread)
        self.btn_restore.pack(side="left", expand=True, fill="x", padx=10, ipady=10)

        info_label = ttk.Label(root, text="支持格式: Word (.docx), Excel (.xlsx), PDF (.pdf)\nPDF 文档将进行文本抽取并重构为 DOCX 格式。", foreground="gray", justify="center")
        info_label.pack(pady=5)

        # 3. 日志面板
        log_frame = ttk.LabelFrame(root, text=" 运行日志 ", padding=10)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, state='disabled', font=("Consolas", 9))
        self.log_text.pack(fill="both", expand=True)

        sys.stdout = Logger(self.log_text)
        sys.stderr = Logger(self.log_text)

        print("[SYSTEM] UI framework initialized. Waiting for task dispatch...")

    def select_input(self):
        path = filedialog.askdirectory()
        if path: self.input_path.set(path)

    def select_output(self):
        path = filedialog.askdirectory()
        if path: self.output_path.set(path)

    def run_desensitize_thread(self):
        self.btn_mask.config(state="disabled")
        self.btn_restore.config(state="disabled")
        threading.Thread(target=self.run_desensitize, daemon=True).start()

    def run_restore_thread(self):
        self.btn_mask.config(state="disabled")
        self.btn_restore.config(state="disabled")
        threading.Thread(target=self.run_restore, daemon=True).start()

    def run_desensitize(self):
        try:
            in_dir = self.input_path.get()
            out_dir = self.output_path.get()

            if not os.path.exists(in_dir):
                print(f"[ERROR] Input directory missing: {in_dir}")
                return

            print("\n[SYSTEM] Booting privacy decision engine...")
            db = SecureDB(DB_PATH, MASTER_KEY)
            processor = DocumentProcessor(NER_MODEL_PATH, "unused", db)

            files = [f for f in os.listdir(in_dir) if not f.startswith("~$")]
            print(f"[SYSTEM] Total files in queue: {len(files)}")

            for filename in files:
                in_p = os.path.join(in_dir, filename)
                out_p = os.path.join(out_dir, "脱敏_" + filename)
                ext = filename.lower().split('.')[-1]

                if ext in ['docx', 'doc']:
                    processor.process_docx(in_p, out_p)
                elif ext in ['xlsx', 'xls']:
                    processor.process_excel(in_p, out_p)
                elif ext == 'pdf':
                    processor.process_pdf(in_p, out_p)
                else:
                    print(f"[WARNING] Unrecognized extension: {filename}")

            print("\n[SYSTEM] Batch processing complete.")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[EXCEPTION] System fault: {e}")
        finally:
            self.root.after(0, self.enable_buttons)

    def run_restore(self):
        try:
            target_dir = self.output_path.get()

            print(f"\n[SYSTEM] Booting decryption and restoration engine...")
            restorer = Restorer(DB_PATH, MASTER_KEY)

            if not os.path.exists(target_dir):
                print(f"[ERROR] Target directory missing: {target_dir}")
                return

            files = [f for f in os.listdir(target_dir) if not f.startswith("~$") and not f.startswith("还原_")]

            if not files:
                print("[WARNING] No masked files detected in the target directory.")
                return

            for f in files:
                in_p = os.path.join(target_dir, f)
                new_name = f.replace("脱敏_", "还原_", 1) if f.startswith("脱敏_") else "还原_" + f
                out_p = os.path.join(target_dir, new_name)
                ext = f.lower().split('.')[-1]

                if ext == 'docx':
                    if restorer.process_docx(in_p, out_p):
                        restorer.log_operation(f, "SUCCESS", "Manual trigger via GUI")
                elif ext in ['xlsx', 'xls']:
                    if restorer.process_excel(in_p, out_p):
                        restorer.log_operation(f, "SUCCESS", "Manual trigger via GUI")

            print("\n[SYSTEM] Restoration batch complete.")

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[EXCEPTION] System fault: {e}")
        finally:
            self.root.after(0, self.enable_buttons)

    def enable_buttons(self):
        self.btn_mask.config(state="normal")
        self.btn_restore.config(state="normal")


if __name__ == "__main__":
    root = tk.Tk()
    style = ttk.Style()
    style.theme_use('clam')
    app = PrivacyApp(root)
    root.mainloop()