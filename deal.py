# -*- coding: utf-8 -*-
"""
@Description: Enron 英文邮件语料预处理脚本。执行文本清洗、非法字符滤除，并批量切割合并至 DOCX 格式。
"""

import pandas as pd
from docx import Document
import os
import re

CSV_PATH = "./input_life/emails.csv"
OUTPUT_DIR = "./input_life"
EMAILS_PER_DOCX = 5000


def remove_control_characters(text):
    """
    数据清洗：利用正则剥离导致 XML 解析异常的非标准 ASCII 控制字符
    允许：\x09 (Tab), \x0A (LF), \x0D (CR)
    """
    if not isinstance(text, str):
        return ""
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)


def convert_all_emails():
    if not os.path.exists(CSV_PATH):
        print(f"[ERROR] Source file not found: {CSV_PATH}")
        return

    print(f"[INFO] Initializing batch conversion. Batch size: {EMAILS_PER_DOCX} records.")
    chunk_iterator = pd.read_csv(CSV_PATH, chunksize=EMAILS_PER_DOCX)

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for i, chunk in enumerate(chunk_iterator):
        file_index = i + 1
        doc = Document()
        doc.add_heading(f'Enron Email Dataset - Batch {file_index}', 0)

        print(
            f"[INFO] Generating subset {file_index} (Records: {i * EMAILS_PER_DOCX} - {(i + 1) * EMAILS_PER_DOCX})...")

        for index, row in chunk.iterrows():
            content = row.get('message', '')

            # 执行数据截断防止单个文档过大引发内存溢出
            clean_content = remove_control_characters(str(content))[:5000]

            if clean_content.strip():
                try:
                    doc.add_paragraph(f"--- Email ID: {index} ---")
                    doc.add_paragraph(clean_content)
                except Exception as e:
                    print(f"[WARNING] Skipping row {index} due to parsing error: {e}")

        output_path = os.path.join(OUTPUT_DIR, f"Enron_Batch_{file_index:03d}.docx")
        doc.save(output_path)
        print(f"[SUCCESS] Exported subset to {output_path}")


if __name__ == "__main__":
    convert_all_emails()