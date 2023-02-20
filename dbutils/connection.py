import os
import sqlalchemy



def _ladda_variabler(databas : str, miljövariabler : list) -> dict:
    ''' Laddar och lägger till namn på databas samt miljövariablerna listade i 
        "miljövariabler" i "vars".

        Parametrar:
            databas  : Databas mot vilken uppkoppling ska ske 
            var_namn : Lista med de miljövariabler som ska laddas

        Returns:
            vars : Värde på miljövariabler samt namn på databas  
    '''

    vars = {'DB' : databas}

    for var in miljövariabler:
        värde = os.environ.get(var)
        if värde is None:
            raise ValueError(f'Miljövariabel "{var}" saknas. Se till att miljövariabler för \
                            MySQL-Server är definierade.')

        vars[var] = värde
    
    return vars


def gen_mysql_connection(databas : str):
    ''' Skapa koppling mot MySQL-databas.

        Parametrar:
            databas : Aktuell databas/schema på MySQL-Server

        Returns:
            sqlalchemy.engine.base.Connection  
    '''

    miljövariabler = ['MYSQL_USR', 'MYSQL_PWD', 'MYSQL_HOST']

    vars = _ladda_variabler(databas, miljövariabler)

    db_uri = 'mysql+pymysql://{MYSQL_USR}:{MYSQL_PWD}@{MYSQL_HOST}/{DB}?local_infile=1'.format(**vars)
    engine = sqlalchemy.create_engine(db_uri)
    return engine.connect()








def gen_postgresql_connection(databas : str='postgres'):
    ''' Skapa koppling mot PostgreSQL-databas.

        Parametrar:
            databas : Aktuell databas/schema på PostgreSQL-Server. I nuläget har vi bara 
                      en databas på Postgres-servern med namn "postgres"

        Returns:
            sqlalchemy.engine.base.Connection  
    '''

    miljövariabler = ['PG_USR', 'PG_PWD', 'PG_HOST']

    vars = _ladda_variabler(databas, miljövariabler)

    db_uri = "postgresql://{PG_USR}:{PG_PWD}@{PG_HOST}:5432/{DB}".format(**vars)
    engine = sqlalchemy.create_engine(db_uri)  
    return engine.connect()


# #FIXME - TA BORT? Lär inte ha några behov av en Engine, sannolikt är Connection allt som behövs
# def gen_postgresql_engine(databas : str='postgres'):
#     ''' Skapa koppling mot PostgreSQL-databas.

#         Parametrar:
#             databas : Aktuell databas/schema på PostgreSQL-Server. I nuläget har vi bara 
#                       en databas på Postgres-servern med namn "postgres"

#         Returns:
#             sqlalchemy.engine.base.Engine  
#     '''

#     miljövariabler = ['PG_USR', 'PG_PWD', 'PG_HOST']

#     vars = _ladda_variabler(databas, miljövariabler)

#     db_uri = "postgresql://{PG_USR}:{PG_PWD}@{PG_HOST}:5432/{DB}".format(**vars)
#     engine = sqlalchemy.create_engine(db_uri)  

#     return engine



