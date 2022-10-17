from captura_credenciais import retorna_credenciais
from capturar_dados_de_venda_rds import captura_dados_de_venda_cliente
# from insere_dados_documentDB import insere_dados_DocDB
# from captura_iqvia import iqvia_DB
import pandas as pd

import time

beg = time.time()

def main():

    # Coletando as credenciais para os RDSs dos clientes.
    print('Coletando as credenciais dos clientes', end = '\n ', flush = True)
    credenciais = retorna_credenciais()
    print('temp:{}'.format((time.time()-beg)/60))
    print('Credenciais coletadas', end = '\n ', flush = True)
    print(' ', end = '\n', flush = True)
    
    final = pd.DataFrame()

    for cliente in[ credenciais[1]]:
        print(credenciais)

        # Capturando dados de venda
        print('Iniciando operação do cliente {}'.format(cliente[6]), end = '\n', flush = True)
        print('Capturando dados cliente', end = '\n ', flush = True)
        print(' ', end = '\n', flush = True)
        df_transacoes = captura_dados_de_venda_cliente(cliente)
        print(df_transacoes.head())
        print('temp:{}'.format((time.time()-beg)/60), end = '\n ')
        print('Dados de venda capturados. Começando a captura do iqvia', end = '\n ', flush = True)
        print(' ', end = '\n', flush = True)



        break


        
        # lista_ean = df_transacoes['ean'].unique()
        # # print(len(lista_ean))
        # db_iqvia = iqvia_DB(False, lista_ean)
        # print('temp:{}'.format((time.time()-beg)/60))
        # print('Dados do iqvia capturados. Construindo a base final', end = '\n', flush = True)
        # print(' ', end = '\n', flush = True)
        # # print(db_iqvia.shape)
        # df_transacoes = df_transacoes.merge(db_iqvia, on = 'ean', how='left')

        # """ TO DO: Nem todos os eans que estão aqui estão na nossa tabela IQVIA. Temos que trabalhar em cima dos eans que não 
        # forem encontrados. """
        # # print(df_transacoes[df_transacoes['classe4'].isna()].head())

        # df_transacoes['rede'] = cliente[-2]

        # df_transacoes['qtdvendida'] = df_transacoes['qtdvendida'].astype(float)
        # # Inserindo o numero da rede
        # # print(df_transacoes.columns, end = '\n', flush = True)
        # # print(df_transacoes.head(), end = '\n', flush = True)
        # # print(df_transacoes.shape, end = '\n', flush = True)
        # print('temp:{}'.format((time.time()-beg)/60))
        # print('Base final pronta, indo para o próximo cliente', end = '\n', flush = True)
        # print(' ', end = '\n', flush = True)
        
        # final = pd.concat([final,df_transacoes])


    # return insere_dados_DocDB(False, final)
    
    return None




if __name__ == "__main__":
# For interactive work (on ipython) it's easier to work with explicit objects
# instead of contexts.
    main()    