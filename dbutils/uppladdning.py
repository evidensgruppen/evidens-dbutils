import pandas as pd
from typing import Union

from .connection import gen_mysql_connection, gen_postgresql_engine

#FIXME - I nuläget fungerar inte uppladdning till postgres
def ladda_upp(
    df      : pd.DataFrame, 
    tabell  : str, 
    databas : str,
    server  : str='mysql',
    dtypes  : Union[dict, None]=None
) -> None:
    ''' Laddar upp df till vald databas. Uppladdning bygger i hög grad på pandas to_sql()-funktion. Vissa tillägg har
        gjorts för att passa våra behov, bl.a. laddas data inte direkt upp till tabellen "tabell" utan först till 
        "tabell_tmp". Detta för att eventuella befintliga tabeller inte ska ligga ner under uppladdningstiden. 

        Argument:
            df      : Dataframe som ska laddas upp
            tabell  : Namn på tabell som ska genereras 
            databas : Namn på databas/schema
            server  : Den server på vilken databasen ligger/ska ligga
            dtypes  : Datatyper för kolumnerna i df
    '''

    # Tillåtna servrar och korresponderande anslutningsfunktioner
    _servers = {
        'mysql'    : gen_mysql_connection, 
        'postgres' : gen_postgresql_engine
    }

    # Se till att argumentet "server" har ett tillåtet värde 
    if server not in _servers:
        raise ValueError('Argument "server" måste vara antingen "{0}" eller "{1}"'.format(*_servers.keys()))

    # Anslut till vald server
    _con_fun = _servers[server]
    con      = _con_fun(databas)

    # Se till att indexkolumn har unika värden
    df = df.reset_index(drop=True)

    # Ta bort tmp-tabell ifall sådan finns
    tmp_tabell = tabell + '_tmp'
    con.execute(f'DROP TABLE IF EXISTS {tmp_tabell}')

    print(f'\nLaddar upp data till "{tmp_tabell}"')

    sql_params = {
        'name'        : tmp_tabell,
        'con'         : con,
        'if_exists'   : 'replace', 
        'chunksize'   : 10000, 
        'index_label' : 'id'
    }

    if server=='postgres':
        sql_params['schema']='public'



    # Ladda upp data till ny tabell med suffix "_tmp"
    # if schema is None:
        # Om inga datatyper specificerats väljs dessa automatiskt av pandas
        # df.to_sql(tmp_tabell, con=con, if_exists='replace', chunksize=10000, index_label='id')

    # else:
        # Om datatyper specificerats används keys för att se till att kolumnerna i df kommer i rätt ordning
        # df = df[list(schema.keys())]
        # df.to_sql(tmp_tabell, con=con, if_exists='replace', chunksize=10000, dtype=schema, index_label='id')  


    if dtypes is not None:
        df = df[list(dtypes.keys())]
        sql_params['dtype']=dtypes

    df.to_sql(**sql_params)

    # Ersätt befintlig tabell med den data som just laddades upp till "tmp_tabell"
    con.execute(f'DROP TABLE IF EXISTS {tabell}')
    con.execute(f'ALTER TABLE {tmp_tabell} RENAME TO {tabell}')
    
    print(f'\nTabell "{tabell}" uppdaterad')
