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



