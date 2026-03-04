import pandas as pd
import os

ADULT_FILE_PATH = "./downloads/adult/adult.data"
HEART_DIR = "./downloads/disease"

OUTPUT_DIR = "./input_file"


def convert_adult_dataset():
    """
    处理 Adult 人口普查数据集
    特点：逗号分隔，有空格，无表头
    """
    print(f"正在处理 Adult 数据集: {ADULT_FILE_PATH} ...")

    if not os.path.exists(ADULT_FILE_PATH):
        print(f"错误：找不到文件 {ADULT_FILE_PATH}，请检查路径！")
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
        print(f"Adult 转换成功！已保存至: {save_path} (行数: {len(df)})")

    except Exception as e:
        print(f"Adult 转换失败: {e}")


def convert_all_heart_datasets():
    """
    批量处理 UCI Heart Disease 的所有 4 个子数据集
    包括：Cleveland, Hungarian, Switzerland, Long Beach VA
    """
    print(f"开始扫描心脏病数据集目录: {HEART_DIR} ...")

    if not os.path.exists(HEART_DIR):
        print(f"找不到文件夹 {HEART_DIR}")
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

    count = 0
    for filename in target_files:
        file_path = os.path.join(HEART_DIR, filename)

        if not os.path.exists(file_path):
            print(f"找不到文件 {filename}")
            continue

        print(f"Processing: {filename} ...")

        try:
            df = pd.read_csv(file_path, header=None, names=columns, na_values="?", skipinitialspace=True)

            clean_name = filename.replace("processed.", "").replace(".data", "").capitalize()
            if clean_name == "Va": clean_name = "Long_Beach_VA"

            save_name = f"Heart_Disease_{clean_name}.xlsx"
            save_path = os.path.join(OUTPUT_DIR, save_name)

            df.to_excel(save_path, index=False)
            print(f" 转换成功: {save_path} (行数: {len(df)})")
            count += 1

        except Exception as e:
            print(f" 转换失败 {filename}: {e}")

    print(f"🎉 心脏病数据集处理完毕！共生成 {count} 个 Excel 文件。")


if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"📂 已创建输出目录: {OUTPUT_DIR}\n")
    convert_adult_dataset()
    convert_all_heart_datasets()