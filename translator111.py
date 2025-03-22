import csv
import argparse
import asyncio
import re
import pandas as pd
from tqdm import tqdm
from loguru import logger
from google_trans_new import google_translator  # 适用于 async

# 全局变量
SEMAPHORE = asyncio.Semaphore(20)  # 限制并发
translator = google_translator()  # 使用非官方异步翻译库

# 识别含有中文的列
async def containChinese(col, threshold=0.2):
    async with SEMAPHORE:
        count = sum(bool(re.search(r'[\u4e00-\u9fff]', str(text))) for text in col.dropna())
        return count / len(col) if count / len(col) > threshold else 0

async def containChineseColumn(df, threshold=0.2):
    tasks = {col: containChinese(df[col], threshold) for col in df.columns}
    results = await asyncio.gather(*tasks.values())
    chinese_cols = [col for col, result in zip(tasks.keys(), results) if result]
    logger.info(f"Detected Chinese columns: {chinese_cols}")
    return chinese_cols

# 异步翻译
async def translate_text(text):
    async with SEMAPHORE:
        try:
            result = await asyncio.to_thread(translator.translate, text, lang_src='zh-cn', lang_tgt='en')
            return result.strip()
        except Exception as e:
            logger.error(f"Error translating text: {e}")
            return text  # 出错时返回原文本

# 处理单行
async def translate_row(row, fields_to_translate):
    tasks = {field: translate_text(row[field]) for field in fields_to_translate if row[field]}
    results = await asyncio.gather(*tasks.values())
    for field, result in zip(tasks.keys(), results):
        row[field] = result
    return row

# 处理整个数据集
async def translate_dataset(input_file: str, output_file='', fields_to_translate=[]):
    if not output_file:
        output_file = input_file.replace('.csv', '_translated.csv')
    
    df = pd.read_csv(input_file)
    
    if not fields_to_translate:
        logger.error("No fields to translate. Exiting.")
        return

    logger.info(f"Translating fields: {fields_to_translate}")
    
    rows = df.to_dict(orient="records")  # 转换为字典列表
    tasks = [translate_row(row, fields_to_translate) for row in rows]
    
    translated_rows = await tqdm_asyncio_gather(tasks)  # 并发执行
    translated_df = pd.DataFrame(translated_rows)
    
    translated_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logger.info(f"Translation complete. Output saved to {output_file}")

# tqdm 适配 asyncio
async def tqdm_asyncio_gather(tasks):
    results = []
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Translating"):
        results.append(await future)
    return results

# 命令行参数处理
def main():
    parser = argparse.ArgumentParser(description='Async CSV Translator using Google Translate API')
    parser.add_argument('input_file', type=str, help='Input CSV file')
    parser.add_argument('--output_file', type=str, default='', help='Output CSV file')
    parser.add_argument('--threshold', type=float, default=0.2, help='Threshold for detecting Chinese text')
    parser.add_argument('--fields', type=str, nargs='+', default=[], help='Fields to translate')
    args = parser.parse_args()

    try:
        df = pd.read_csv(args.input_file)
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return

    fields = args.fields or asyncio.run(containChineseColumn(df, args.threshold))
    
    asyncio.run(translate_dataset(args.input_file, args.output_file, fields))

if __name__ == '__main__':
    main()
