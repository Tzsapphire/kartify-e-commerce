#!/usr/bin/env python

# ## Kartify e-commerce at https://api-production-8692.up.railway.app/

# >> import modules:

from sqlalchemy import inspect
import pandas as pd

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


# # to call a table: pg_table_dict[dataframe name]

if __name__ == "__main__":
    pg_table_dict = extract(src_engine=postgres_conn, schema_name="public")

    print(f"you are connected to the {postgres_db} database")
    print(pg_table_dict.keys())