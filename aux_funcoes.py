#!/usr/bin/env python
# coding: utf-8

# In[ ]:

#
# definição das bibliotecas necessárias
#


#
# gera lista de arquivos no diretório
#
import requests as rq
from bs4 import BeautifulSoup as bs
def get_url_paths(url, ext='', params={}):
    response = rq.get(url, params=params)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = bs(response_text, 'html.parser')
    parent = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return parent

#
# define função de tempo
#
from datetime import datetime as dt
def agora():    
    # datetime object containing current date and time
    now = dt.now()
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    #print("data e hora = ", dt_string)
    return dt_string


# https://stackoverflow.com/questions/4130922/how-to-increment-datetime-by-custom-months-in-python-without-using-library
#import datetime as dt
import calendar as cal
def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, cal.monthrange(year,month)[1])
    return dt.datetime(year, month, day, 0, 0)


#
# define função de limpeza de tabelas
#
def limpa_tabela(conn, tabela):
    sql = 'delete from '
    cur = conn.cursor()
    cur.execute(sql + tabela)
    conn.commit()
    #print(sql + tabela)
    
def otimizarSQLite(conn):
    conn.cursor().execute('VACUUM;')

#
# https://linuxhint.com/filter_list_strings_python/
# Filter a list of string using another list and custom function
# Declare a funtion to filter data from the first list
def filtra_lista(lista_principal, lista_filtro):
    return [n for n in lista_principal if
             any(m in n for m in lista_filtro)]


import pandas as pd
def rentabilidade(dataset: pd.DataFrame, data_ref: str='', descricao: str='12 meses') -> pd.DataFrame:
    df = dataset
    #RECRIA INDICE
    df = df.reset_index(drop=True)
    
    df.loc[0, 'RENTAB_COTA'] = (df.loc[0,'VAR_COTA'] + 1) *1
    for i in range(1, len(df)):
        df.loc[i, 'RENTAB_COTA'] = (df.loc[i,'VAR_COTA'] + 1) * df.loc[i-1, 'RENTAB_COTA']

    # PEGA O ULTIMO VALOR CALCULADO
    rentab_cota = df.loc[i, 'RENTAB_COTA']-1
    
    # contrói uma lista
    data = [{'CNPJ_FUNDO':df.loc[i, 'CNPJ_FUNDO'], 'DT_ULT_PREGAO': data_ref, 'JANELA':descricao, 'RENTABILIDADE':rentab_cota}]
  
    # cria um datframe usando a lista
    returns = pd.DataFrame(data)
    return returns                                   

def selic_mensal(dataset: pd.DataFrame, data_ref: str='', periodo: str='') -> pd.DataFrame:
    df = dataset
    #RECRIA INDICE
    df = df.reset_index(drop=True)
    
    df.loc[0, 'RENTAB_SELIC'] = (df.loc[0,'TAXA'] + 1) *1
    for i in range(1, len(df)):
        df.loc[i, 'RENTAB_SELIC'] = (df.loc[i,'TAXA'] + 1) * df.loc[i-1, 'RENTAB_SELIC']

    # PEGA O ULTIMO VALOR CALCULADO
    rentab = df.loc[i, 'RENTAB_SELIC']-1
    
    # contrói uma lista
    data = [{'MES_SELIC': data_ref, 'TX_SELIC':rentab}]
  
    # cria um datframe usando a lista
    returns = pd.DataFrame(data)
    return returns 

