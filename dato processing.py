from clickhouse_connect import get_client 
import pandas as pd 
from datetime import datetime 
from rapidfuzz import fuzz, process 
 
DF1_COLS = ['uid','full_name','email','address','sex','birthdate','phone'] 
DF2_COLS = ['uid','first_name','middle_name','last_name','birthdate','phone','address'] 
DF3_COLS = ['uid','name','email','birthdate','sex'] 
 
client = get_client(host='clickhouse', port=8123) 
 
 
 
def load_table(table_name, columns, df_name="df"): 
    print(f"{datetime.now()} {df_name}") 
    offset = 0 
    limit = 1000000 
    while True: 
        table_dataset = client.query(f'SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}') 
        result_rows = table_dataset.result_rows 
        if not result_rows: 
            break  
        df = pd.DataFrame(result_rows, columns=columns) 
        offset += limit  
        print(offset) 
 
    return df 
 
 
 
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
     
    print("Данные успешно записаны в таблицу table_results") 
 
 
def clear_table(table_name='table_results'): 
    client = get_client(host='clickhouse', port=8123) 
    clear_query = f'TRUNCATE TABLE {table_name};' 
    client.command(clear_query) 
    print(f"Таблица {table_name} очищена") 
 
 
 
 
 
 
print(f"{datetime.now()} df start") 
df1 = load_table('table_dataset1', DF1_COLS, 'df1') 
print(df1.head()) 
df2 = load_table('table_dataset2', DF2_COLS, 'df2') 
df3 = load_table('table_dataset3', DF3_COLS, 'df3') 
print(f"{datetime.now()} df end") 
print(df3.head()) 
 
df = df1.applymap(lambda s: s.lower() if type(s) == str else s) 
df2 = df2.applymap(lambda s: s.lower() if type(s) == str else s) 
df3 = df3.applymap(lambda s: s.lower() if type(s) == str else s) 
 
 
 
count = 70 
results = [] 
 
df2_full_names = df2['first_name'] + ' ' + df2['middle_name'] + ' ' + df2['last_name'] 
df3_full_names = df3['name'] 
 
for go, i in enumerate(df2_full_names): 
 
    matches_df = process.extract(i, df['full_name'], scorer=fuzz.WRatio, limit=5) 
 
 
    for match_df in matches_df: 
        j = match_df[0] 
        name_similarity = match_df[1] 
        name_similarity2 = fuzz.token_sort_ratio(i, j) 
        name_similarity_all = (name_similarity2 + name_similarity) / 2 
 
 
        if name_similarity_all >= count: 
 
            matches_df3 = process.extract(i, df3_full_names, scorer=fuzz.WRatio, limit=5) 
 
 
            for match_df3 in matches_df3: 
                k = match_df3[0] 
                name_similarity3 = match_df3[1] 
                name_similarity4 = fuzz.token_sort_ratio(i, k) 
                name_similarity_all3 = (name_similarity3 + name_similarity4) / 2 
 
                if name_similarity_all3 >= count: 
                    results.append(pd.concat([ 
                        df.loc[df['full_name'] == j], 
                        df2.iloc[[go]], 
                        df3.loc[df3['name'] == k] 
                    ], axis=1)) 
 
 
if results: 
    df_full_concat = pd.concat(results, ignore_index=True) 
 
df_full_concat 
 
count = 75 
result_fuzzy = [] 
 
 
full_names = df_full_concat['full_name'] 
 
for i, name_i in enumerate(full_names): 
    matches = process.extract(name_i, full_names, scorer=fuzz.WRatio, limit=5) 
    for match in matches: 
        name_j = match[0] 
        name_similarity = match[1] 
 
        if name_similarity >= count and name_i != name_j: 
            result_fuzzy.append({ 
                'name_1': name_i, 
                'name_2':
                name_j, 
                'similarity': name_similarity 
            }) 
 
if result_fuzzy: 
    result_fuzzy_df = pd.DataFrame(result_fuzzy) 
else: 
    result_fuzzy_df = pd.DataFrame(columns=['name_1', 'name_2', 'similarity']) 
 
result_fuzzy_df.to_csv('result_fuzzy.csv', index=False) 
 
 
 
write_result(result_fuzzy_df) 
# print(df_result.head())