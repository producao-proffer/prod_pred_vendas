from database.database_client import DatabaseClient

def iqvia_DB(data_science, lista_eans:list):

    DBClient = DatabaseClient()

    documentdb = DBClient.iqvia_info(lista_eans)

    return documentdb