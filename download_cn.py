# -*- coding: utf-8 -*-
"""
@Description: 中文实体识别语料预处理工具。支持 BIO 与 BMES 标注格式的语料序列化构建。
"""

import os
import docx

WORK_DIR = "./input_processcn"
FILE_WEIBO = "weiboNER_2nd_conll.train"
FILE_RESUME = "train.char.bmes"


def save_to_word(texts, filename_prefix, title):
    """序列化导出 DOCX，构建测试语料库"""
    if not texts:
        print(f"[WARNING] Empty data array for {filename_prefix}, skipped.")
        return

    texts = list(set([t.strip() for t in texts if len(t.strip()) > 2]))
    total = len(texts)
    print(f"[INFO] Serialization task initialized: {total} unique records.")

    if not os.path.exists("./input_file"):
        os.makedirs("./input_file")

    doc = docx.Document()
    doc.add_heading(title, 0)

    for i, t in enumerate(texts):
        doc.add_paragraph(f"{t}")

    save_path = f"./input_file/{filename_prefix}.docx"
    doc.save(save_path)
    print(f"[SUCCESS] Exported to: {save_path}")


def process_bio_file(filename, output_prefix, title):
    """解析 BIO/BMES 格式并合并字符边界构建原句"""
    file_path = os.path.join(WORK_DIR, filename)
    print(f"[INFO] Parsing sequence data: {filename} ...")

    if not os.path.exists(file_path):
        print(f"[ERROR] File missing: {file_path}")
        return

    texts = []
    current_chars = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    if current_chars:
                        sentence = "".join(current_chars)
                        if len(sentence) > 1:
                            texts.append(sentence)
                        current_chars = []
                else:
                    parts = line.split()
                    if parts:
                        current_chars.append(parts[0])

            if current_chars:
                texts.append("".join(current_chars))

        save_to_word(texts, output_prefix, title)

    except Exception as e:
        print(f"[ERROR] Parsing exception: {e}")


def main():
    print(f"[SYSTEM] Pipeline start. Work directory: {os.path.abspath(WORK_DIR)}")

    if not os.path.exists(WORK_DIR):
        print(f"[ERROR] Target directory not found: {WORK_DIR}")
        return

    process_bio_file(FILE_WEIBO, "Weibo_Social_Text", "Weibo Social Corpus")
    process_bio_file(FILE_RESUME, "Resume_Entity_Text", "Chinese Resume Dataset")


if __name__ == "__main__":
    main()