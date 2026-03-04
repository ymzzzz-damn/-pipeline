# -*- coding: utf-8 -*-
"""
@Description: 
    预训练模型自动化拉取脚本。
    从 HuggingFace Hub 下载指定的 NER 模型权重及配置文件，
    并固化至本地目录，以支持后续的边端设备离线脱敏任务。
"""

import os

try:
    from huggingface_hub import snapshot_download
except ImportError:
    print("[ERROR] 缺少依赖库，请先在终端运行: pip install huggingface_hub")
    exit(1)
MODEL_ID = "Davlan/distilbert-base-multilingual-cased-ner-hrl"
LOCAL_DIR = "./model_ner_multilingual"

def main():
    print(f"[SYSTEM] 目标模型: {MODEL_ID}")
    print(f"[SYSTEM] 保存路径: {os.path.abspath(LOCAL_DIR)}")
    
    if not os.path.exists(LOCAL_DIR):
        os.makedirs(LOCAL_DIR)
        
    try:
        print("[INFO] 正在连接 HuggingFace 服务器拉取权重 (约 400~500MB，请耐心等待)...")
        
        # snapshot_download 会智能断点续传，并只下载核心文件
        snapshot_download(
            repo_id=MODEL_ID,
            local_dir=LOCAL_DIR,
            local_dir_use_symlinks=False,  
            ignore_patterns=["*.msgpack", "*.h5", "*.ot", "*.flax", "README.md"] 
        )
        
        print(f"\\n[SUCCESS] 模型 [{MODEL_ID}] 拉取完成！")
        
    except Exception as e:
        print(f"\\n[ERROR] 下载失败，请检查网络设置。\\n详细异常信息: {e}")

if __name__ == "__main__":
    main()
