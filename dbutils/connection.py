import os
import sqlalchemy


def gen_mysql_connection(db : str):
    '''Skapa koppling mot MySQL-databas'''

    usr  = os.environ.get('MYSQL_USR')
    pwd  = os.environ.get('MYSQL_PWD')
    host = os.environ.get('MYSQL_HOST')

    db_uri = f'mysql+pymysql://{usr}:{pwd}@{host}/{db}?local_infile=1'
    engine = sqlalchemy.create_engine(db_uri)
    return engine.connect()





