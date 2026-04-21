#!/usr/bin/env python

# ## Kartify e-commerce at https://api-production-8692.up.railway.app/

# >> import modules:
# from sqlalchemy.engine import URL as sa_URL
from sqlalchemy import create_engine, text, inspect
# from snowflake.sqlalchemy import URL as sf_URL
import os , pandas as pd
# from dotenv import load_dotenv
# load_dotenv('.env')

# get details from the Connections
from connections import pg_engine  



postgres_conn, postgres_db = pg_engine()


#  Extracting tables from the source database
def extract(src_engine, schema_name):
    print("engine connection created?, analysing tables:")
    inspector = inspect(src_engine)
    # schema_name = input("\n what schema should we look at? ")
    table_names = inspector.get_table_names(schema = schema_name)
    print("\n the tables present in", schema_name, "are the following: ", table_names, "\n")
    dict_list={}
    for tables in table_names:
        df= pd.read_sql(f'select * from "{schema_name}"."{tables}"', src_engine)
        file_name = f"{tables}_df"
        dict_list[file_name] = df #store as key and vaariables
        # globals()[file_name] = df  #store the dfs in the global space
    print("tables renamed in the dictionary as dataframes \n", dict_list.keys(), "\n")
    return dict_list


pg_table_dict = extract(src_engine=postgres_conn, schema_name="public")

print(f"you are connected to the {postgres_db} database")
print(pg_table_dict.keys())
# to call a table: pg_table_dict[datafarme name]
















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