import os
from typing import Union
import pandas as pd

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
        "tabell_tmp". Detta för att eventuella befintliga tabeller inte ska ligga ner under uppladdningstiden. Notera
        att funktionen ersätter eventuellt redan existerande tabell med samma namn.

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
        sql_params['dtype'] = dtypes

    print(f'\nLaddar upp data till "{tmp_tabell}"')
    df.to_sql(**sql_params)

    # Ersätt befintlig tabell med den data som just laddades upp till "tmp_tabell"
    con.execute(f'DROP TABLE IF EXISTS {tabell}')
    con.execute(f'ALTER TABLE {tmp_tabell} RENAME TO {tabell}')

    print(f'\nTabell "{tabell}" uppdaterad')




def bulkuppladdning(
    df      : pd.DataFrame, 
    tabell  : str, 
    databas : str,
    dtypes  : Union[dict, None]=None
) -> None:
    ''' Snabb uppladdning av stora dataset genom uppladdning av textfil. Data i 
        df sparas först lokalt i temporär textfil som sedan kan laddas upp genom 
        MySQL-kommandot "LOAD DATA LOCAL INFILE ...". Notera att funktionen ersätter 
        eventuellt redan existerande tabell med samma namn.

        Argument:
            df      : Dataframe som ska laddas upp
            tabell  : Namn på tabell som ska genereras 
            databas : Namn på databas/schema
            dtypes  : Datatyper för kolumnerna i df
    '''
    
    # Skapa databaskoppling
    con = gen_mysql_connection(databas)

    # Namn på temporär textfil som ska laddas upp till databas
    fil = 'tmp.txt'

    # Default os.linesep är '\n' för Linux och '\r\n' för Windows, line_terminator måste 
    # därför specificeras för at uppladdning ska fungera för båda systemen
    line_terminator  = '\r\n'
    field_terminator = '\t'

    # Se till att indexkolumn får unika värden
    df = df.reset_index(drop=True)

    if dtypes is not None:
        # Se till att kolumner i df har samma ordning som tabell (specificeras i schema)
        df = df[list(dtypes.keys())]

    # Ladda upp data till en temporär tabell
    tmp_tabell = tabell + '_tmp'

    # Skapar tom tabell med rätt kolumnnamn och datatyper
    head_ = df.head(0).copy()
    head_.to_sql(tmp_tabell, con, if_exists='replace', dtype=dtypes, index_label='id')

    # Spara df till temporär textfil, sätt Nans till 'NULL' i exporterad fil för att inte få 
    # nollor i float-kolumner där värden saknas
    df.to_csv(fil, encoding="utf-8", sep=field_terminator, line_terminator=line_terminator, na_rep='NULL')

    # Ladda upp textfil till destinationtabell 
    query = f"""
        LOAD DATA LOCAL INFILE '{fil}' 
        INTO TABLE {tmp_tabell} 
        CHARACTER SET UTF8 
        FIELDS TERMINATED BY '{field_terminator}' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '{line_terminator}'
        IGNORE 1 ROWS
    """

    print(f"\nLaddar upp data till tabell '{tmp_tabell}'")
    con.execute(query)

    # Byt namn på tenporär tabell
    con.execute(f'DROP TABLE IF EXISTS {tabell}')
    con.execute(f'ALTER TABLE {tmp_tabell} RENAME TO {tabell}')

    # Ta bort den temporära fil som genererades inför uppladdning
    os.remove(fil)

    print(f"\nTabell '{tabell}' uppdaterad\n")





