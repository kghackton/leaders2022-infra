import json
import pandas as pd
import sqlalchemy
import psycopg2

from sqlalchemy.sql import text
from sqlalchemy.types import Integer, Text, String, DateTime, VARCHAR
from tqdm import tqdm

path = "./Full_16_09_22.csv"
url_path = "./db_url.json"

with open(url_path) as json_file:
    dic = json.load(json_file)

url = dic["dialect+driver"] + "://" + dic["username"] + ":" + dic["password"] + "@" + dic["host"] + ":" + dic["port"] + "/" + dic["database"]

engine = sqlalchemy.create_engine(url)

id_features = [
    "root_id",
    "version_id",
    "request_number",
    "mos_ru_request_number",
    "root_identificator_of_maternal",
    "number_of_maternal",
    "deffect_category_id",
    "deffect_id",
    "district_code",
    "hood_code",
    "performing_company",
    "inn_of_performing_company",
    "id_of_reason_for_decline", # all values are NaN in taken sample
    "id_of_reason_for_decline_of_org",   # all values are NaN in taken sample
    "id_work_type_done",
    "id_guarding_events"
]

date_features = [
    "date_of_creation",
    "date_of_start",
    "wanted_time_from",
    "wanted_time_to",
    "date_of_review",
    "date_of_last_return_for_revision",
    "closure_date"
]

# All numerical features are discrete -> casting to Int64
numerical_features = [
    "times_returned",    # might be useful
    "adress_unom"
]

categorical_features = [
    "name_of_source",
    "name_of_source_eng",
    "name_of_creator",
    "role_of_user",
    "deffect_category_name",
    "deffect_category_name_eng",
    "deffect_name",
    "short_deffect_name",
    "need_for_revision",
    "urgency_category",
    "urgency_category_eng",
    "district",
    "hood",
    "adress_of_problem",
    "incident_feature",
    "serving_company",
    "dispetchers_number",
    "request_status",
    "request_status_eng",
    "reason_for_decline",   # all values are NaN in taken sample
    "reason_for_decline_of_org", # all values are NaN in taken sample
    "efficiency",
    "efficiency_eng",
    "being_on_revision",
    "alerted_feature",
    "grade_for_service",
    "grade_for_service_eng",
    "payment_category",
    "payment_category_eng",
    "payed_by_card"
]

# String features can also be categorical with a lot of possible values
string_features = [
    "last_name_redacted",
    "commentaries", # might be useful
    "code_of_deffect",
    "description",  # might be useful
    "presence_of_question", # might be useful

    "dispetchers_number",
    "owner_company",
    "work_type_done", # might be useful
    "used_material",
    "guarding_events", # might be useful
    "review",    # might be useful - here is the information about the results of the work

    # These are numerical features, that include some strangely filled rows:
    "floor",
    "flat_number",
    "porch",
]

dtype = {}
for feature in string_features + categorical_features + id_features:
    dtype[feature] = VARCHAR
for feature in numerical_features:
    dtype[feature] = Integer
for feature in date_features:
    dtype[feature] = DateTime

def change_columns(df, naming_path="naming.csv") -> pd.DataFrame:
    '''
    Function which changes column names for pd.DataFrame
    '''
    
    new_names = list(
        map(
            lambda x: x.strip(),
            list(pd.read_csv(naming_path)['new_name'])
        )
    )
    df.columns = new_names

    return df

# Изменение типов колонок:
def cast_types(df) -> pd.DataFrame:

    to_str = string_features + categorical_features + id_features

    for feature in to_str:
        df = df.astype({
            feature: "object"
        })

    for feature in numerical_features:
        df = df.astype({
            feature: "Int64"
        })

    for feature in date_features:
        df = df.astype({
            feature: "datetime64[ns]"   # if the time precission is given up to ns - that's the format
        })
    
    return df

def write_into_db(df, dtype, if_exists="append", name="requests", bd_chunksize=1000) -> None:
    '''
    Functions which writes pd.DataFrame into DB

    if_exists: append/replace/fail
    '''
    df.to_sql(
    name, 
    con = engine,
    if_exists = if_exists,   
    index=False,
    chunksize=bd_chunksize,
    dtype = dtype
    )

def write_by_chunks_into_db(path, engine, iterator_chunksize=5000, bd_chunksize=1000, steps=1, replace_all_data=False, skip_amount=0) -> None:
    '''
    добавление в бд iterator_chunksize * steps строк

    path: 'global' path to huge csv file
    steps: количество шагов
    iterator_chunksize: размер чанка для 1 кусочка таблицы
    bd_chunksize: сколько за раз записывается в бд

    skip_amount: сколько строчек надо скипнуть и потом начать записывать

    '''
    wrote_smth_into_db = False

    # Последовательно можно доставать куски:
    df_iterator = pd.read_csv(
        path,
        sep='$',
        chunksize=iterator_chunksize,
        low_memory=False
    )
    for chunk_number, chunk in tqdm(enumerate(df_iterator), desc="Iter: "):
        
        chunk = cast_types(change_columns(chunk))

        if skip_amount and ((chunk_number + 1) * iterator_chunksize <= skip_amount or chunk_number == 0):
            continue

        if not wrote_smth_into_db and replace_all_data:
            write_into_db(chunk, dtype=dtype, if_exists='replace', bd_chunksize=bd_chunksize)
            wrote_smth_into_db = True
        else:
            write_into_db(chunk, dtype=dtype, if_exists='append', bd_chunksize=bd_chunksize)

# write_by_chunks_into_db(
#     path,
#     engine,

#     iterator_chunksize=20000,
#     steps=80,   # здесь надо учитывать skip_amount: Если надо закинуть N строк, скипнув M строк, то: steps * iterator_chunksize == M + N

#     bd_chunksize=20000,

#     # replace_all_data=False,

#     skip_amount = 1000000 + 200000
# )
