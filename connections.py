# define connections

    # install packages (run on terminal)
# pip3 install psycopg2-binary; 
# pip3 install sqlalchemy ; create_engine, inspect, text  #from sqlalchemy.engine import URL
# pip3 install snowflake-connector-python;  
# pip3 install --upgrade snowflake-sqlalchemy; #from snowflake.sqlalchemy import URL
# pip3 install python-dotenv; python -m pip install python-dotenv 


# >> import modules:
from sqlalchemy.engine import URL as sa_URL
from sqlalchemy.sql import quoted_name
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL as sf_URL
import os 
from dotenv import load_dotenv
load_dotenv('.env')


# creating source database connection function

def src_pg_engine():
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
        return src_engine, pg_database
    except Exception as e:
        print('error connecting to database, details: ', e)



# creating destination snowflake warehouse connection function

def dest_sf_engine(schemaname):
    dest_url = sf_URL(
        account = os.getenv("SNOWFLAKE_ACCOUNT") ,
        user = os.getenv("SNOWFLAKE_USER") ,
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        schema = schemaname,
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role= os.getenv("SNOWFLAKE_ROLE")
    )
    try:
        print('creating engine connection to destination... \n')
        dest_sf_eng = create_engine(dest_url)
        print('testing destination database connection...')
        with dest_sf_eng.connect() as dest_conn:
           db_query =text('select CURRENT_DATABASE();')
           schema_query =text('select CURRENT_SCHEMA();')
           dest_db = dest_conn.execute(db_query).scalar()
           schema_db = dest_conn.execute(schema_query).scalar()
           print(f'connection to {dest_db} and {schema_db} successful \n')
        return dest_sf_eng
    except Exception as e:
        print('connection failed, error is: ', e)

# raw_schema_eng = sf_engine(os.getenv("SCHEMA_BRONZE"))
# silver_schema_eng = sf_engine(os.getenv("SCHEMA_SILVER"))
# gold_schema_eng = sf_engine(os.getenv("SCHEMA_GOLD"))


# creating destination postgres connection
# create postgres engine
def dest_pg_engine():
    dest_pg_url = sa_URL.create(
        drivername='postgresql+psycopg2',
        username=os.getenv("dest_username"),
        password = os.getenv("dest_password"),
        host=os.getenv("dest_host"),
        database=os.getenv("dest_database"),
        port =os.getenv("dest_port")
    )
    try: 
        dest_pg_eng = create_engine(dest_pg_url)
        with dest_pg_eng.begin() as dest_pg_conn: #use begin to ensure autocommit
            db_query=text('select CURRENT_DATABASE();')     # do i create the database, where do i save it in variable?
            pg_database= dest_pg_conn.execute(db_query).scalar()
            # dest_pg_schema = quoted_name(newschema, quote=True) #quotedname to prevent inserts
            # schemaname= newschema
            # schemaname='raw'
            newschema= input(' \n what schema to create? ') 
            schema_query=text(f'create schema if not exists "{newschema}";')
            dest_pg_conn.execute(schema_query)
            print("connected to database: ", pg_database, "and schema: ", newschema )
        return dest_pg_eng, newschema
    except Exception as e:
        print('connection failed, error is: ', e)
        # return None, None


# next
if __name__ == "__main__":
    src_pg_engine()
    raw_schema_eng = dest_sf_engine(os.getenv("SCHEMA_BRONZE"))
    silver_schema_eng = dest_sf_engine(os.getenv("SCHEMA_SILVER"))
    gold_schema_eng = dest_sf_engine(os.getenv("SCHEMA_GOLD"))

    a, b = src_pg_engine()
    print(a, '\n', b)

    c, d = dest_pg_engine()
    print(c, '\n', d)



