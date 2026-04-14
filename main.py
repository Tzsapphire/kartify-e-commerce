#!/usr/bin/env python

# ## Kartify e-commerce at https://api-production-8692.up.railway.app/

# pip install snowflake-connector-python, psycopg2, --upgrade snowflake-sqlalchemy

# >> import modules:
from sqlalchemy.engine import URL as sa_URL
from sqlalchemy import create_engine, text, inspect
from snowflake.sqlalchemy import URL as sf_URL
import os , pandas as pd
from dotenv import load_dotenv
load_dotenv('.env')


# creating source database connection function

def pg_engine():
    pg_sa_url = sa_URL.create(
        drivername="postgresql+psycopg2",
        username= os.getenv("postgres_user"),
        password= os.getenv("postgres_password"),
        host= os.getenv("postgres_host"),
        port=os.getenv("postgres_port"),
        database= os.getenv("postgres_database")
    )
    print('creating engine connection to source... \n')
    try:
        src_engine = create_engine(pg_sa_url)
        with src_engine.connect() as pg_conn:
            query=text('select CURRENT_DATABASE();')
            print('testing database connection...')
            pg_database = pg_conn.execute(query).scalar()
            print("connected to database: ", pg_database, "\n")
        return src_engine
    except Exception as e:
        print('error connecting to database, details: ', e)


variable1 = pg_engine()


#  Extracting tables from the source database
def extract(src_engine, schema_name):
    print("engine connection created?, analysing tables:")
    inspector = inspect(src_engine)
    schema_name = input("\n what schema should we look at? ")
    table_names = inspector.get_table_names(schema = schema_name)
    print("\n the tables present in your schema are the following: ", table_names, "\n")
    dict_list={}
    for tables in table_names:
        df= pd.read_sql(f'select * from "{schema_name}"."{tables}"', src_engine)
        file_name = f"{tables}_df"
        dict_list[file_name] = df #store as key and vaariables
        # globals()[file_name] = df  #store the dfs in the global space
    print("keys in this 'dict_list' dictionary \n", dict_list.keys(), "\n")
    return dict_list

variable2 = extract(src_engine=..., schema_name=...)
print({variable2}.keys())















# # establishing the source engine
# pg_src_engine = create_engine(
#     'postgresql+psycopg2://db_user:db_password@db_host:db_port/db_name'
#     )



# # establishing the destination warehouse
# conn = snowflake.connector.connect(
#     user=USER,
#     password=PASSWORD,
#     account=ACCOUNT,
#     warehouse=WAREHOUSE,
#     database=DATABASE,
#     schema=SCHEMA
#     )


# snowflake_dest_engine = {
#     user=os.get_env("DB_USER"),
#     password=PASSWORD os.get_env("DB_PASSWORD"),
#     account=ACCOUNT os.get_env("DB_ACCOUNT"),
#     warehouse=os.get_env("DB_WAREHOUSE"),
#     database=os.get_env("DB_DATABASE"),
#     schema=os.get_env("DB_SCHEMA")
# }