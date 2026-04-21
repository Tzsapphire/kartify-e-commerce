
# import the various functions and modules needed here:
import os
from connections import pg_engine  
from extract import extract


# connecting to the source database:
postgres_conn, postgres_db = pg_engine()
# extracting the tables and saving in a dictionary
pg_table_dict = extract(src_engine=postgres_conn, schema_name=os.getenv("pg_schema"))

print(f"you are connected to the {postgres_db} database")
print(pg_table_dict.keys())


# #### transform the customers table

# in_prog_customers = pg_table_dict[customers_df].copy()
