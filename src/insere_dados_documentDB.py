from database.database_client import DatabaseClient

def insere_dados_DocDB(lista_transacoes:list):

    # print(lista_transacoes, flush = True, end = '\n')

    DBClient = DatabaseClient()


    documentdb = DBClient.insert_documentdb(lista_transacoes)

    return documentdb