import pandas as pd
from database.database_client import DatabaseClient
from config.app_config import Config

def retorna_credenciais():
    """
    Essa funcao faz retorna as credenciais dos clientes da Proffer.

    Parameters
        ----------
       
    """

    lista_conexao = [Config.DATABASE_TENANT_HOSTNAME, 
    Config.DATABASE_TENANT_PORT, 
    Config.DATABASE_TENANT_DBNAME,
    Config.DATABASE_TENANT_USERNAME,
    Config.DATABASE_TENANT_PASSWORD]
                   

    # Extrai as transacoes pareto do banco de dados, e registra documentdb da aws. 
    sql_acessos_base = "select * from credentials.database"

    DBClient = DatabaseClient()
    df = DBClient.query_to_df(sql_acessos_base, lista_conexao)

    return df