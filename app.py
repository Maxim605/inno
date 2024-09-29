from clickhouse_connect import get_client
import logging

import re
import pandas as pd
import numpy as np
from datetime import datetime
from rapidfuzz import fuzz, process
import os



def load_all_parquet(folder_path):
    dataframes = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.parquet'):
            file_path = os.path.join(folder_path, filename)
            df = pd.read_parquet(file_path)  
            dataframes.append(df)  

    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df

def write_result(df):
    client = get_client(host='clickhouse', port=8123)
    
    create_table_query = '''
    CREATE OR REPLACE TABLE table_results
    (
        id_is1 Array(UUID),
        id_is2 Array(UUID),
        id_is3 Array(UUID)
    )
    ENGINE = MergeTree()
    ORDER BY id_is1;
    '''
    client.command(create_table_query)
    insert_query = '''
    INSERT INTO table_results (id_is1, id_is2, id_is3) VALUES
    '''
    values = []
    for index, row in df.iterrows():
        values.append(f"({row['id_is1']}, {row['id_is2']}, {row['id_is3']})")
    insert_query += ', '.join(values) + ';'
    client.command(insert_query)
    
    print("Данныео записаны в table_results")


dtypes1 = {
    'uid': 'string',
    'full_name': 'string',
    'email': 'string',
    'address': 'string',
    'sex': 'category',
    'birthdate': 'string',
    'phone': 'string'
}

dtypes2 = {
    'uid': 'string',
    'first_name': 'string',
    'middle_name': 'string',
    'last_name': 'string',
    'birthdate': 'string',
    'phone': 'string',
    'address': 'string'
}

dtypes3 = {
    'uid': 'string',
    'name': 'string',
    'email': 'string',
    'birthdate': 'string',
    'sex': 'category'
}

logging.info(f"{datetime.now()} df1.")
df1 = load_all_parquet('/app/data/main1')
duplicates_df1 = df1[df1.duplicated(subset=['full_name', 'email'], keep=False)]

logging.info(f"{datetime.now()} df2.")
df2 = load_all_parquet('/app/data/main2')
duplicates_df2 = df2[df2.duplicated(subset=['first_name', "middle_name", "last_name", "birthdate"], keep=False)]

logging.info(f"{datetime.now()} df2.")
df3 = load_all_parquet('/app/data/main3')
duplicates_df3 = df3[df3.duplicated(subset=["name", "email", "sex"], keep=False)]


if 'full_name' not in df1.columns: df1['full_name'] = df1['full_name_part1'].fillna('') + ' ' + df1['full_name_part2'].fillna('') + ' ' + df1['full_name_part3'].fillna('')
if 'full_name' not in df2.columns: df2['full_name'] = df2['first_name_part1'].fillna('') + ' ' + df2['middle_name_part1'].fillna('') + ' ' + df2['last_name_part1'].fillna('')
if 'full_name' not in df3.columns: df3['full_name'] = df3['name_part1'] + ' ' + df3['name_part2'] + ' ' + df3['name_part3']

if 'birthdate' not in df2.columns: df2['full_name'] = df2['birth_year'] + ' ' + df2['birth_month'] + ' ' + df2['birth_day']
    
duplicates_df1 = df1[df1.duplicated(subset=['full_name', 'email'], keep=False)]
duplicates_df2 = df2[df2.duplicated(subset=['full_name', 'birthdate'], keep=False)]
duplicates_df3 = df3[df3.duplicated(subset=['full_name', 'email', 'sex'], keep=False)]

count_threshold = 70
results = []

for i, row1 in duplicates_df1.iterrows():
    print(f"\nProcessing full_name: {row1['full_name']} from duplicates_df1")


    match_df2 = process.extractOne(row1['full_name'], duplicates_df2['full_name'], scorer=fuzz.WRatio)
    print(f"Matched with duplicates_df2: {match_df2}")

    if match_df2 and match_df2[1] >= count_threshold:
        matched_row_df2 = duplicates_df2.loc[duplicates_df2['full_name'] == match_df2[0]]
        print(f"Duplicate found in df2: {matched_row_df2['full_name'].values}")

        match_df3 = process.extractOne(row1['full_name'], duplicates_df3['full_name'], scorer=fuzz.WRatio)
        print(f"Matched with duplicates_df3: {match_df3}")

        if match_df3 and match_df3[1] >= count_threshold:
            matched_row_df3 = duplicates_df3.loc[duplicates_df3['full_name'] == match_df3[0]]
            print(f"Duplicate found in df3: {matched_row_df3['full_name'].values}")


            result = [
                [row1['uid']],
                [matched_row_df2['uid'].values[0]] if not matched_row_df2.empty else [],
                [matched_row_df3['uid'].values[0]] if not matched_row_df3.empty else []
            ]
            results.append(result)
            print(f"Result appended: {result}")

if results:
    print("\nFinal Results:")
    for result in results:
        print(result)

    write_result(result)
else:
    print("\nNo matches found.")