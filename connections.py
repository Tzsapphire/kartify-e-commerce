# define connections

#import modules
    # install packages (run on terminal)
# pip3 install psycopg2-binary; pip3 install sqlalchemy ; pip3 install pandas
# pip3 install snowflake-connector-python; pip3 install --upgrade snowflake-sqlalchemy


from sqlalchemy import create_engine, inspect
import pandas as pd


def pg_engine():
    db_user = input("whats your postgres db username?")
    db_password = input("whats your db password?")
    db_host = input("whats your db host?")
    db_port = input("whats your db port?")
    db_name = input("whats your db name?")
    print("creating your source engine...")
    try:
        engine_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        src_engine = create_engine(engine_url)
        print('engine created, attempting connection...')
        # you have to try to connect 
        with src_engine.connect():
            print("connection successful:", src_engine.connect)
        return src_engine
    except Exception as e:
        print("error connecting to database, error:", e)
    return None



def extract(src_engine):
    print("engine connection created?, analysing tables:")
    inspector = inspect(src_engine)
    schema_name = input("what schema should we look at?")
    table_names = inspector.get_table_names(schema = schema_name)
    print("the tables present in your schema are the following:", table_names)
    dict_list={}
    for tables in table_names:
        df= pd.read_sql(f'select * from "{schema_name}"."{tables}"', src_engine)
        file_name = f"{tables}_df"
        dict_list[file_name] = df #store as key and vaariables
        # globals()[file_name] = df  #store the dfs in the global space
    print("keys in this dictionary", dict_list.keys())
    return dict_list




default_connection_name = "myaccount"
def sf_engine():
    sf_user = input("whats your snowflake user login name?")
    sf_password = input("whats your sf user password?")
    sf_account = input("whats your sf account identifier ?")
    sf_db_name = input("whats your sf db_name?")
    print("creating your destination engine...")
    try:
        engine_url = f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_db_name}'
        dest_engine = create_engine(engine_url)
        print('engine created, attempting connection...')
        # you have to try to connect 
        with dest_engine.connect() as sf_conn:
            sf_conn.execute('select current_version(), CURRENT_DATABASE(), CURRENT_SCHEMA()')
            print("connection successful:", sf_conn)
        return dest_engine
    except Exception as e:
        print("error connecting to database, error:", e)
    return None

try_pg = pg_engine()
print(try_pg)

try_sf_engine = sf_engine()
print(try_sf_engine)
