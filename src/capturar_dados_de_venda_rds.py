# To do:
# O timedelta pode variar de empresa para empresa. Temos que tornar mais generico. 

import pandas as pd
from database.database_client import DatabaseClient
from datetime import datetime, timedelta


def captura_dados_de_venda_cliente(conn_info:list):
    """
    Essa funcao faz retorna os dados de um determinado cliente. Esses dados serao inseridos na tabela de treinamento em producao.

    Parameters
        ----------
        conn_info : list, mandatory
            lista de informacoes para conectar na base. A sequencia devera ser host, porta, database, username, password
    """

    # Extrai as transacoes pareto do banco de dados, e registra documentdb da aws.
    sql = """
    select
       p.ean as ean,
       v.codproduto as codproduto,
       v.codloja as codloja,
       extract(year from date_trunc('week',TO_DATE(datavenda,'YYYY-MM-DD'))) as ano,
       extract(week from TO_DATE(datavenda,'YYYY-MM-DD')) as semana,
       sum(v.qtdvendida) as qtdvendida
    from dbo.venda v
    left join dbo.produto p on v.codproduto = p.codproduto
    left join dbo.produtoloja pl on v.codproduto = pl.codproduto
    where (TO_DATE(datavenda,'YYYY-MM-DD') >= date(cast(now() as date) + INTERVAL '-2 week'))
    and (v.precovenda > 0) and (pl.aplica_teste = True)
    group by 
        p.ean,
        v.codproduto,
       extract(year from date_trunc('week',TO_DATE(datavenda,'YYYY-MM-DD'))),
       extract(week from TO_DATE(datavenda,'YYYY-MM-DD')),
       v.codloja
    """

    DBClient = DatabaseClient()
    df = DBClient.query_to_df(sql, conn_info)
    col_list = ['ean', 'codproduto', 'codloja', 'ano', 'semana', 'qtdvendida']
    df = pd.DataFrame(df, columns = col_list)

    df['ean'] = df['ean'].astype(float)

    

    return df