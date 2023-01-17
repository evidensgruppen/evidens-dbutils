import pandas as pd
from typing import Union

from .connection import gen_mysql_connection

#FIXME - I nuläget fungerar inte uppladdning till postgres
def ladda_upp(
    df      : pd.DataFrame, 
    tabell  : str, 
    databas : str,
    dtypes  : Union[dict, None]=None
) -> None:
    ''' Laddar upp df till vald databas. Uppladdning bygger i hög grad på pandas to_sql()-funktion. Vissa tillägg har
        gjorts för att passa våra behov, bl.a. laddas data inte direkt upp till tabellen "tabell" utan först till 
        "tabell_tmp". Detta för att eventuella befintliga tabeller inte ska ligga ner under uppladdningstiden. 

        Argument:
            df      : Dataframe som ska laddas upp
            tabell  : Namn på tabell som ska genereras 
            databas : Namn på databas/schema
            dtypes  : Datatyper för kolumnerna i df
    '''

    # Anslut till databas
    con = gen_mysql_connection(databas)

    # Se till att indexkolumn har unika värden
    df = df.reset_index(drop=True)

    # Ta bort tmp-tabell ifall sådan finns
    tmp_tabell = tabell + '_tmp'
    con.execute(f'DROP TABLE IF EXISTS {tmp_tabell}')

    # Parametrar för uppladdning
    sql_params = {}

    sql_params['name']        = tmp_tabell
    sql_params['con']         = con
    sql_params['chunksize']   = 10000
    sql_params['if_exists']   = 'replace'
    sql_params['index_label'] = 'id'

    if dtypes is not None:
        # Om datatyper specificerats används keys för att se till att kolumnerna i df kommer i rätt ordning
        df = df[list(dtypes.keys())]
        sql_params['dtype']=dtypes

    print(f'\nLaddar upp data till "{tmp_tabell}"')
    df.to_sql(**sql_params)

    # Ersätt befintlig tabell med den data som just laddades upp till "tmp_tabell"
    con.execute(f'DROP TABLE IF EXISTS {tabell}')
    con.execute(f'ALTER TABLE {tmp_tabell} RENAME TO {tabell}')
    
    print(f'\nTabell "{tabell}" uppdaterad')





