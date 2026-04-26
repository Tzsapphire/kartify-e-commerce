
# import the various functions and modules needed here:
import os
from connections import src_pg_engine, dest_pg_engine
from extract import extract


# connecting to the source database:
print('\n connecting to source database')
postgres_conn, postgres_db = src_pg_engine()

# extracting the tables and saving in a dictionary
print('\n extract tables into a dictionary')
pg_table_dict = extract(src_engine=postgres_conn, schema_name=os.getenv("pg_schema"))

print(f"you are connected to the {postgres_db} database")
print(pg_table_dict.keys())

#after extracting and saving the tables in a dictionary send to the db
print('\n connecting to destination database')

dest_postgres_eng, dest_postgres_schema = dest_pg_engine()

print(f" \n you are sending to the {dest_postgres_schema} schema")

print('\n sending tables to the destination database')
for title, table in pg_table_dict.items():
    table_df = table.to_sql(title, dest_postgres_eng, schema=dest_postgres_schema, if_exists= 'fail', index=False)
    print(f"{title} table has been uploaded") 


