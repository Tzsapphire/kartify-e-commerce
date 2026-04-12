# define connections

#import modules
    # install packages (run on terminal)
# pip3 install psycopg2-binary; 
# pip3 install sqlalchemy ; create_engine, inspect, text  #from sqlalchemy.engine import URL
# pip3 install snowflake-connector-python;  
# pip3 install --upgrade snowflake-sqlalchemy; #from snowflake.sqlalchemy import URL
# pip3 install python-dotenv; python -m pip install python-dotenv 


# >> PART TWO:

from sqlalchemy.engine import URL as sa_URL
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL as sf_URL
import os 
from dotenv import load_dotenv
load_dotenv('conn_setup.env')


# creating source engine connection function

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


pg_engine()

