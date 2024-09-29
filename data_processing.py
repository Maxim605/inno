
import logging
from clickhouse_connect import get_client
import re
import os
import numpy as np
import pandas as pd
from datetime import datetime       


DF1_COLS = ['uid', 'full_name', 'email', 'address', 'sex', 'birthdate', 'phone'] 
DF2_COLS = ['uid', 'first_name', 'middle_name', 'last_name', 'birthdate', 'phone', 'address']
DF3_COLS = ['uid', 'name', 'email', 'birthdate', 'sex']


def load_and_save_batches(table_name, columns, df_name, batch_size=1000000, folder_path='/app/data/', cols_to_split=[]):
    offset = 0
    batch_num = 1
    while True:
        logging.info(f"{datetime.now()} Загрузка батча {batch_num} из {df_name}")
        query = f'SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}'
        table_dataset = client.query(query)
        result_rows = table_dataset.result_rows
        if not result_rows:
            break
        
        df = pd.DataFrame(result_rows, columns=columns)
        df = clean_data(df, cols_to_split)
        
        batch_file_path = f"{folder_path}/{df_name}/batch_{batch_num}.parquet"
        df.to_parquet(batch_file_path, index=False)
        logging.info(f"{datetime.now()} Батч {batch_num} сохранен: {batch_file_path}")
        
        offset += batch_size
        batch_num += 1

def process_phone(phone):
    phone_clean = re.sub(r'\D', '', phone)
    operator_code = phone_clean[1:4] if len(phone_clean) > 4 else ''
    main_number = phone_clean[4:] if len(phone_clean) > 4 else ''
    return operator_code, main_number

def remove_repeated_characters(text):
    return re.sub(r'(.)\1+', r'\1', text)

def clean_data(df, cols_to_split):
    df = df.apply(lambda s: s.str.lower() if s.dtype == 'object' and s.name not in ['uid', 'sex', 'phone', 'birthdate'] else s)
    
    for col in cols_to_split:
        if col in df and col not in ['uid', 'sex', 'phone', 'birthdate']:
            df[col] = df[col].apply(
                lambda x: remove_repeated_characters(
                    re.sub(r'[^a-zA-Zа-яА-ЯёЁ\s]', '', x) 
                )
            )
    for col in cols_to_split:
        if col in df and col not in ['uid', 'sex', 'phone', 'birthdate']: 
            split_cols = df[col].str.split(' ', n=2, expand=True)
            expected_col_count = len(split_cols.columns)
            
            for i in range(expected_col_count):
                if f"{col}_part{i+1}" not in df:
                    df[f"{col}_part{i+1}"] = split_cols[i]
                    
    if 'birthdate' in df:
        df['birth_year'] = pd.to_numeric(df['birthdate'].str[:4], errors='coerce').abs()
        df['birth_month'] = pd.to_numeric(df['birthdate'].str[5:7], errors='coerce').abs()
        df['birth_day'] = pd.to_numeric(df['birthdate'].str[8:10], errors='coerce').abs()
        df['birth_month'] = df['birth_month'].fillna(0)
        df['birth_year'] = df['birth_year'].fillna(0)  
        df['birth_day'] = df['birth_day'].fillna(0)
    if 'phone' in df:
        df['phone_operator_code'], df['phone_main_number'] = zip(*df['phone'].apply(process_phone))
    if 'sex' in df:
        df['sex'] = df['sex'].map({'m': 1, 'f': 0}).fillna(np.nan)
    
    df['uid'] = df['uid'].astype(str)
    df = df.fillna('')
    # df = df.drop(columns=[col for col in cols_to_split if col != 'uid'] + ['email', 'birthdate', 'phone'], errors='ignore')
    df['birth_year'] = pd.to_numeric(df['birth_year'], errors='coerce').fillna(0).astype(int)
    
    return df


os.makedirs('/app/data/main1', exist_ok=True)
os.makedirs('/app/data/main2', exist_ok=True)
os.makedirs('/app/data/main3', exist_ok=True)
logging.basicConfig(level=logging.INFO)
client = get_client(host='clickhouse', port=8123)
logging.info(f"{datetime.now()} Начало обработки df1")
load_and_save_batches('table_dataset1', DF1_COLS, 'main1', cols_to_split=['full_name'])
logging.info(f"{datetime.now()} df1 обработан и сохранен батчами")
logging.info(f"{datetime.now()} Начало обработки df2")
load_and_save_batches('table_dataset2', DF2_COLS, 'main2', cols_to_split=['first_name', 'middle_name', 'last_name'])
logging.info(f"{datetime.now()} df2 обработан и сохранен батчами")

logging.info(f"{datetime.now()} Начало обработки df3")
load_and_save_batches('table_dataset3', DF3_COLS, 'main3', cols_to_split=['name'])
logging.info(f"{datetime.now()} df3 обработан и сохранен батчами")

logging.info(f"{datetime.now()} Все датафреймы обработаны и сохранены батчами успешно.")

