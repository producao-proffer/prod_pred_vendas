"""
This module contains all the classes and functions related to database handling
"""

# from urllib import parse, quote
import psycopg2
import pymongo
import urllib
import pandas as pd
from typing import List
from config.app_config import Config
from decimal import Decimal
from bson.decimal128 import Decimal128

def query_rds(sql, conn_list):
    

    """

    Parameters
        ----------
    """

    try:
        conn = psycopg2.connect(
                    user=conn_list[3],
                    password = conn_list[4],
                    host = conn_list[0], 
                    port= conn_list[1],
                    database = conn_list[2]
                    )


                            # Get a database cursor
        cur = conn.cursor()

        # Execute SQL
        cur.execute(sql)
        # Get the result
        result = cur.fetchall()

        conn.close()
        return result

    except Exception as ex:
        print(ex)

    # return None


def query_iqvia(lista_eans):

    user = Config.DOCUMENTDB_USERNAME
    password = urllib.parse.quote_plus(Config.DOCUMENTDB_PASSWORD)
    db = Config.DOCUMENTDB_DB_IQVIA
    host = Config.DOCUMENTDB_HOST
    port = Config.DOCUMENTDB_PORT
    pem_path = Config.DATABASE_TENANT_PEM_PATH

    ##Create a MongoDB client, open a connection to Amazon DocumentDB as a replica set and specify the read preference as secondary preferred
    client = pymongo.MongoClient(f'mongodb://{user}:{password}@{host}:{port}/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false') 

    ##Specify the database to be used
    db = client[db]

    # Collection name
    col = db["iqvia"]


    lista_info = []
    for ean in lista_eans:
        filter = {'ean': ean}
        cursor = col.find(filter)
        for doc in cursor:
            lista_info.append(doc)

    df_iqvia = pd.DataFrame(lista_info)
    # print('##################################')
    # print(' ')
    # print(df_iqvia.columns)
    # print(' ')
    # print('##################################')
    
    
    return df_iqvia[['ean','classe4','forma3','fcc']]



def convert_decimal(dict_item):
    # This function iterates a dictionary looking for types of Decimal and converts them to Decimal128
    # Embedded dictionaries and lists are called recursively.
    if dict_item is None: return None

    for k, v in list(dict_item.items()):
        if isinstance(v, dict):
            convert_decimal(v)
        elif isinstance(v, list):
            for l in v:
                convert_decimal(l)
        elif isinstance(v, Decimal):
            dict_item[k] = Decimal128(str(v))

    return dict_item


def inserir_document_db(info):
    # print(df_de_transacoes)
    user = Config.DOCUMENTDB_USERNAME
    password = urllib.parse.quote_plus(Config.DOCUMENTDB_PASSWORD)
    db = Config.DOCUMENTDB_DB_ELASTICIDADE
    host = Config.DOCUMENTDB_HOST
    port = Config.DOCUMENTDB_PORT
    pem_path = Config.DATABASE_TENANT_PEM_PATH


    ##Create a MongoDB client, open a connection to Amazon DocumentDB as a replica set and specify the read preference as secondary preferred
    client = pymongo.MongoClient(f'mongodb://{user}:{password}@{host}:{port}/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false') 
    # print('ok1', flush = True, end = '\n')
    ##Specify the database to be used
    db = client[db]

    # Collection name
    col = db["previsao_venda"]
    count = 0

    list_of_itens = []

    # print('Removendo', flush = True, end = '\n')
    db.transacao.delete_many({})
    # print('Inserindo', flush = True, end = '\n')
    
    return col.insert_many(info)

class DatabaseClient:
    """
        DatabaseClient abstracts the database connecion
        and query execution procedure.

        Parameters
        ----------

        
    ...

    Attributes
    ----------
    connetion : psycopg2.connect
        a formatted string to print out what the animal says
    name : str
        the name of the animal
    sound : str
        the sound that the animal makes
    num_legs : int
        the number of legs the animal has (default 4)

    Methods
    -------
    says(sound=None)
        Prints the animals name and what sound it makes

    """

    def __init__(self):
        # self.connection = create_connection()
        return None

    def get_connection(self) -> psycopg2.connect:
        """
            Return the current connection to the database.
        """
        return self.connection

    def query_to_df(self, sql: str, lista_conn:list) -> pd.DataFrame:
        """
            Execute a select query on a database and convert the
            result to a pandas dataframe.
        """

        try:
            result= query_rds(sql, lista_conn)



            return result
            
        except Exception as ex:
            print(ex)


    def insert_documentdb(self, df):
        return inserir_document_db(df)

    def iqvia_info(self, list_eans):
        return query_iqvia(list_eans)



