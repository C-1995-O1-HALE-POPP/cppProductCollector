import csv
from googletrans import Translator
from tqdm.asyncio import tqdm
from loguru import logger
import argparse
import asyncio
import pandas as pd
import re
SEMAPHORE = asyncio.Semaphore(20)
translator = Translator()

async def containChinese(col, threshold=0.2):
    async with SEMAPHORE:
        count = sum(bool(re.search(r'[\u4e00-\u9fff]', str(text))) for text in col.dropna())
        return count / len(col) if count / len(col) > threshold else 0

async def containChineseColumn(df, threshold=0.2):
    tasks = {col: containChinese(df[col], threshold) for col in df.columns}
    results = await asyncio.gather(*tasks.values())
    logger.info(f"Chinese columns: {[(col, result) for col, result in zip(tasks.keys(), results) if result]}")
    return [col for col, result in zip(tasks.keys(), results) if result]

async def translate_text(text):
    async with SEMAPHORE:
        try:
            translation = await translator.translate(text, src='zh-cn', dest='en')
            return translation.text
        except Exception as e:
            logger.error(f"Error translating text: {e}")
            return text
        
async def translate_row(row, fields_to_translate):
    translation_tasks = {field: translate_text(row[field]) for field in fields_to_translate if row[field]}
    translated_results = await asyncio.gather(*translation_tasks.values())
    for field, result in zip(translation_tasks.keys(), translated_results):
        row[field] = result 
    return row

async def translate_dataset(input_file: str, output_file = '', fields_to_translate = []):
    if not output_file:
        output_file = input_file.replace('.csv', '_translated.csv')
    if not fields_to_translate:
        return
    
    with open(input_file, 'r', encoding='utf-8', newline='') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        if not fields_to_translate:
            fields_to_translate = reader.fieldnames

        for field in fields_to_translate:
            if field not in reader.fieldnames:
                logger.error(f"Field {field} not found in input file")
                return
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in tqdm([translate_row(row, fields_to_translate) for row in reader]):
            writer.writerow(await row)
    logger.info(f"Translation complete. Output written to {output_file}")
    return

def main():
    parser = argparse.ArgumentParser(description='Translate a CSV file using Google Translate API')
    parser.add_argument('input_file', type=str, help='Input CSV file to translate')
    parser.add_argument('--output_file', type=str, default='', help='Output CSV file to write translated data')
    parser.add_argument('--threshold', type=float, default=0.2, help='Threshold for determining if a text contains Chinese characters')
    parser.add_argument('--fields', type=str, nargs='+', default=[], help='Fields to translate')
    args = parser.parse_args()
    fields = []
    try:
        df = pd.read_csv(args.input_file)
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return
    
    if args.fields:
        fields = args.fields
    else:
        fields = asyncio.run(containChineseColumn(df, args.threshold))
    asyncio.run(translate_dataset(args.input_file, args.output_file, fields))

if __name__ == '__main__':
    main()