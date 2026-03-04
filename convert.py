# -*- coding: utf-8 -*-
"""
@Description: UCI Machine Learning Repository 结构化数据集预处理脚本。
"""

import pandas as pd
import os

ADULT_FILE_PATH = "./downloads/adult/adult.data"
HEART_DIR = "./downloads/disease"
OUTPUT_DIR = "./input_file"


def convert_adult_dataset():
    """Adult 人口普查数据标准化映射方法"""
    print(f"[INFO] Initializing ADULT dataset processing: {ADULT_FILE_PATH}")

    if not os.path.exists(ADULT_FILE_PATH):
        print(f"[ERROR] Data source missing: {ADULT_FILE_PATH}")
        return

    columns = [
        "Age", "Workclass", "fnlwgt", "Education", "Education-Num",
        "Marital-Status", "Occupation", "Relationship", "Race", "Sex",
        "Capital-Gain", "Capital-Loss", "Hours-per-week", "Native-Country", "Income"
    ]

    try:
        df = pd.read_csv(ADULT_FILE_PATH, header=None, names=columns, skipinitialspace=True)
        save_path = os.path.join(OUTPUT_DIR, "Adult_Census_Data.xlsx")
        df.to_excel(save_path, index=False)
        print(f"[SUCCESS] ADULT processing complete. Shape: {df.shape}")
    except Exception as e:
        print(f"[ERROR] ADULT exception: {e}")


def convert_heart_disease_datasets():
    """Heart Disease 多中心数据集标准化映射方法"""
    if not os.path.exists(HEART_DIR):
        print(f"[ERROR] Directory missing: {HEART_DIR}")
        return

    columns = [
        "Age", "Sex", "ChestPainType", "RestingBP", "Cholesterol",
        "FastingBS", "RestingECG", "MaxHR", "ExerciseAngina",
        "Oldpeak", "Slope", "Ca", "Thal", "Target"
    ]

    target_files = [
        "processed.cleveland.data",
        "processed.hungarian.data",
        "processed.switzerland.data",
        "processed.va.data"
    ]

    for filename in target_files:
        file_path = os.path.join(HEART_DIR, filename)

        if not os.path.exists(file_path):
            print(f"[WARNING] Skipping missing file: {filename}")
            continue

        try:
            df = pd.read_csv(file_path, header=None, names=columns, na_values="?", skipinitialspace=True)
            clean_name = filename.replace("processed.", "").replace(".data", "").capitalize()
            if clean_name == "Va":
                clean_name = "Long_Beach_VA"

            save_name = f"Heart_Disease_{clean_name}.xlsx"
            save_path = os.path.join(OUTPUT_DIR, save_name)

            df.to_excel(save_path, index=False)
            print(f"[SUCCESS] {clean_name} processed. Shape: {df.shape}")
        except Exception as e:
            print(f"[ERROR] Processing exception on {filename}: {e}")


if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    convert_adult_dataset()
    convert_heart_disease_datasets()