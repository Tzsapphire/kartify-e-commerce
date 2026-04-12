#!/usr/bin/env python

# ## Kartify e-commerce at https://api-production-8692.up.railway.app/

# pip install snowflake-connector-python, psycopg2, --upgrade snowflake-sqlalchemy

# importing libraries
import pandas as pd, psycopg2
from sqlalchemy import create_engine, inspect 
from snowflake.sqlalchemy import URL
import os, dot-env

# importing database connections
import connections 

# establishing the source engine
pg_src_engine = create_engine(
    'postgresql+psycopg2://db_user:db_password@db_host:db_port/db_name'
    )

# establishing the destination warehouse
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )


snowflake_dest_engine = {
    user=os.get_env("DB_USER"),
    password=PASSWORD os.get_env("DB_PASSWORD"),
    account=ACCOUNT os.get_env("DB_ACCOUNT"),
    warehouse=os.get_env("DB_WAREHOUSE"),
    database=os.get_env("DB_DATABASE"),
    schema=os.get_env("DB_SCHEMA")
}

