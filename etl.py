# -*- coding: utf-8 -*-
"""
Created on Fri Jun 25 11:01:43 2021

@author: Marco Cruz
"""
autor = ' Marco Cruz / marcoacf@gmail.com'
import aux_config as cfg

tt1= '-'*((75-len(cfg.titulo))//2)
tt2=tt1
if ((75-len(cfg.titulo)) % 2) == 1:
    tt2=tt2+'-'

print('-' * 75)
print(tt1+cfg.titulo+tt2)
print(' ' * (75-len(autor))+autor)
print('-' * 75)

# ---------------------------------------------------------------------------
#
# definição das bibliotecas necessárias
#
import sqlite3 as sql
from pathlib import Path
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.9f' % x)
import matplotlib.pyplot as plt
from datetime import datetime
import datetime as dt
from dateutil.relativedelta import relativedelta

import numpy as np

from yahoofinancials import YahooFinancials


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# no warnings of type FutureWarning will be printed from now on

import aux_funcoes as fa
import aux_consultas_sql as csql
import aux_listas as la

from alive_progress import alive_bar
etapas = 20
with alive_bar(etapas, title=cfg.titulo) as bar:

    #print('-- definição das bibliotecas necessárias\n')
    #
    # ---------------------------------------------------------------------------
    bar.text('importação das bibliotecas e recursos necessários')
    bar()


    # ---------------------------------------------------------------------------
    #
    # define arquivos a serem importados
    #
    # Portal Dados Abertos CVM
    #

    #print('-- define arquivos a serem importados\n')
    bar.text('definindo arquivos a serem importados')
    #url_cotas = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
    #url_carac = 'http://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/'
    #ext = 'csv'
    lst_arq_cotas = fa.get_url_paths(cfg.url_cotas, cfg.ext)
    #print('-- Quantidade de arquivos no diretório de cotas: ' +str(len(lst_arq_cotas))+'\n')
    bar.text('Quantidade de arquivos no diretório de cotas: ' +str(len(lst_arq_cotas)))
    lst_arq_carac = fa.get_url_paths(cfg.url_carac, cfg.ext)
    #print('-- Quantidade de arquivos no diretório de características: ' +str(len(lst_arq_carac))+'\n')
    bar.text('Quantidade de arquivos no diretório de características: ' +str(len(lst_arq_carac)))
    #
    # ---------------------------------------------------------------------------

    # ---------------------------------------------------------------------------
    #
    today = datetime.today()
    datem0 = datetime(today.year, today.month, 1).strftime("%Y%m")
    datem1 = datetime(today.year, today.month-1, 1).strftime("%Y%m")
    datem2 = datetime(today.year, today.month-2, 1).strftime("%Y%m")
    lst_meses = (datem0,datem1,datem2)
    lst_arq_cotas = fa.filtra_lista(lst_arq_cotas,lst_meses)
    #print('-- reduzindo fila de arquivo a serem importados aos últimos três meses..\n')
    bar.text('reduzindo fila de arquivo a serem importados aos últimos três meses..')
    #
    # ---------------------------------------------------------------------------
    bar.text('fila de importação de arquivos da CVM definida')
    bar()
    # ---------------------------------------------------------------------------
    #
    #print('-- excluindo dados recentes da tabela de cotas...\n')
    
    conn = sql.connect(cfg.bancodedados)
    with conn:
        cur = conn.cursor()
        
        cur.execute(csql.sqlDelCotas())
        bar.text('excluindo dados recentes da tabela de cotas...')
        
        cur.execute(csql.sqlDelCaracteristicas())
        bar.text('excluindo dados da tabela de características dos fundos...')
        
        cur.execute(csql.sqlDelSelic())
        bar.text('excluindo dados recentes da tabela SELIC...')
        
        cur.execute(csql.sqlDelIbov())
        bar.text('excluindo dados recentes da tabela da IBOVESPA...')
        
        cur.execute(csql.sqlDelRentabilidade())
        bar.text('excluindo dados da tabela de Rentabilidade...')
        
        #res = cur.fetchall()
        #print(res)
    if conn:
        conn.close()
    #
    # ---------------------------------------------------------------------------
    bar()
    # ---------------------------------------------------------------------------
    #
    #print('-- Arquivos disponíveis para importação: ' + str(len(lst_arq_cotas))+'\n')
    bar.text('arquivos disponíveis para importação: ' + str(len(lst_arq_cotas)))
    # ---------------------------------------------------------------------------
    #
    # Cotações dos fundos 
    assunto = ' Cotações dos fundos'
    #print('-'*(75-len(assunto))+assunto)
    #print('-- Inicio da importação dos csvs (cotas)\n')
    bar.text('inicio da importação dos csvs (cotas)')
    df = (pd.read_csv(f, sep=';') for f in lst_arq_cotas)
    df = pd.concat(df)    
    #print('-- Fim da importação dos csvs de cotas\n')
    bar.text('fim da importação dos csvs de cotas')
    bar()

    # drop by Name
    #print('-- Exclui colunas desnecessárias do dataset\n')
    bar.text('exclui colunas desnecessárias do dataset')
    df = df.drop(la.lst_campos_excluir, axis=1)
    bar()
    # converte coluna DT_COMPTC para tipo data
    #print('-- convertendo tipo de dados...\n')
    bar.text('convertendo tipo de dados')
    df['DT_COMPTC'] =  pd.to_datetime(df['DT_COMPTC'], infer_datetime_format=True)
    # cria coluna MES_COMPTC (para auxiliar mais adiante)
    #print('-- criando o mês...\n')
    bar.text('criando o mês no dataset')
    df['MES_COMPTC']=df['DT_COMPTC'].apply(lambda x : x.replace(day=1))
    #Trata caracteres especiais no cnpj
    #print('-- tratando caracteres especiais no CNPJ...\n')
    bar.text('tratando caracteres especiais no CNPJ')
    df['CNPJ_FUNDO']=df['CNPJ_FUNDO'].str.replace(r'\D', '')
    bar()

    ultimo_pregao = df['DT_COMPTC'].max()
    ultimo_pregao
    #
    # encontra a ultima data de cada mês
    #
    df_dt = df.groupby(['MES_COMPTC']).agg({'DT_COMPTC':'max'})
    df_dt = df_dt.reset_index()
    meses = df_dt['MES_COMPTC']
    df_dt = df_dt.drop(['MES_COMPTC'], axis=1)
    df_dt.rename(columns={'DT_COMPTC':'UltimoDiaMes'}, inplace=True)
    #
    # exclui datas desnecessárias do dataframe
    #
    #print('excluindo datas desnecessárias....\n')
    bar.text('excluindo datas desnecessárias')
    df= df[df['DT_COMPTC'].isin(df_dt['UltimoDiaMes'])]
    #
    # refaz o índice
    df.sort_values(by=['CNPJ_FUNDO','DT_COMPTC'], inplace=True, ascending=[True, True])
    df = df.reset_index(drop=True)
    bar()
    #
    # faz carga no banco de dados
    #print('--  carga das cotas atualizadas no SQLite\n')
    bar.text('carga das cotas atualizadas no SQLite')
    conn = sql.connect(cfg.bancodedados)
    df.to_sql('tbl_cotas', conn,  schema=cfg.esquemaBD,index = False, if_exists='append')
    conn.close()
    bar()


    # ---------------------------------------------------------------------------
    #
    # Cadastros dos fundos
    assunto = ' Cadastros dos fundos'
    #print('-'*(75-len(assunto))+assunto)
    #print('-- Inicio da importação dos csvs (caracteristicas)\n')
    bar.text('Inicio da importação dos csvs (caracteristicas)')
    df2 = (pd.read_csv(f, sep=';', encoding = 'cp1252', low_memory=False) for f in lst_arq_carac)
    df2 = pd.concat(df2)
    #print('-- Fim da importação dos csvs  (caracteristicas)\n')
    bar.text('fim da importação dos csvs  (caracteristicas)')
    # drop by Name
    #print('-- Exclui colunas desnecessárias do dataset\n')
    bar.text('exclui colunas desnecessárias do dataset')
    df2 = df2.drop(la.lst_campos_excluir2, axis=1)
    bar()

    #print('-- convertendo tipo de dados...\n')
    bar.text('convertendo tipo de dados')
    # converte coluna DT_COMPTC para tipo data
    df2['DT_COMPTC'] =  pd.to_datetime(df2['DT_COMPTC'], infer_datetime_format=True)
    # cria coluna MES_COMPTC (para auxiliar mais adiante)
    df2['MES_COMPTC']=df2['DT_COMPTC'].apply(lambda x : x.replace(day=1))
    #Trata caracteres especiais no cnpj
    #print('-- tratando caracteres especiais no CNPJ...\n')
    bar.text('tratando caracteres especiais no CNPJ...')
    df2['CNPJ_FUNDO']=df2['CNPJ_FUNDO'].str.replace(r'\D', '')
    bar()
    
    # exporta csv
    #df2.to_csv('tbl_caracteristicas_01_full.csv', index=False)
    
    
    #
    # encontra a ultima data de cada mês
    #
    #df_dt2 = df2.groupby(['MES_COMPTC']).agg({'DT_COMPTC':'max'})
    #df_dt2 = df_dt2.reset_index()
    #df_dt2 = df_dt2.drop(['MES_COMPTC'], axis=1)
    #df_dt2.rename(columns={'DT_COMPTC':'UltimoDiaMes'}, inplace=True)
    #
    # exclui datas desnecessárias do dataframe
    #
    #print('excluindo datas desnecessárias....\n')
    #bar.text('excluindo datas desnecessárias...')
    #df2 = df2[df2['DT_COMPTC'].isin(df_dt2['UltimoDiaMes'])]
    
    bar.text('classificando datas...')
    df2.sort_values(['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True, ascending=[True, True])

    # exporta csv
    #df2.to_csv('tbl_caracteristicas_02_mes.csv', index=False)
    
    #
    # remove cnpjs duplicados
    #
    df2.drop_duplicates(subset=['CNPJ_FUNDO'], keep='last', inplace=True)
    #
    # refaz o índice
    df2.sort_values(by=['CNPJ_FUNDO','DT_COMPTC'], inplace=True, ascending=[True, False])
    df2 = df2.reset_index(drop=True)
    bar()

    # exporta csv
    #df2.to_csv('tbl_caracteristicas_03_remdupl.csv', index=False)
    #
    # faz carga no banco de dados
    #print('--  carga dos cadastros (fundos) no SQLite\n')
    bar.text('carga dos cadastros (fundos) no SQLite')
    conn = sql.connect(cfg.bancodedados)
    df2.to_sql('tbl_caracteristicas', conn,  schema=cfg.esquemaBD,index = False, if_exists='append')
    conn.close()
    bar()




    # ---------------------------------------------------------------------------
    #
    #  Cotações da taxa SELIC
    assunto = ' Cotações da taxa SELIC'
    #print('-'*(75-len(assunto))+assunto)
    #print('-- Inicio da importação da SELIC diária\n')
    bar.text('início da importação da SELIC diária')
    selic = pd.read_json(cfg.url_selic.format(11))
    bar()

    #print('-- tratando as datas...\n')
    bar.text('tratando as datas e valores...')
    selic['data'] = pd.to_datetime(selic['data'], format = '%d/%m/%Y')
    selic['valor'] = selic['valor']/100 
    bar()

    #calculates decimal rate from the percentual value
    #print('-- renomeando campos...\n')
    bar.text('renomeando campos...')
    selic.rename(columns = {'data':'DT_COTACAO', 'valor':'TAXA'}, inplace = True)
    #selic=selic.style.format({'TAXA':'{:10.9f}'})

    # cria coluna MES_COMPTC (para auxiliar mais adiante)
    #print('-- criando a coluna mês...\n')
    bar.text('criando a coluna mês...')
    selic['MES_COTACAO']=selic['DT_COTACAO'].apply(lambda x : x.replace(day=1))
    selic=selic[['DT_COTACAO','TAXA','MES_COTACAO']]
    bar()
    #
    # encontra a ultima data de cada mês
    #selic_dt = selic.groupby(['MES_COTACAO']).agg({'DT_COTACAO':'max'})
    #selic_dt = selic_dt.reset_index()
    #meses = selic_dt['MES_COTACAO']
    #selic_dt = selic_dt.drop(['MES_COTACAO'], axis=1)
    #selic_dt.rename(columns={'DT_COTACAO':'UltimoDiaMes'}, inplace=True)
    #
    # exclui datas desnecessárias do dataframe
    #print('excluindo datas desnecessárias....')
    #bar.text('excluindo datas desnecessárias...')
    #selic= selic[selic['DT_COTACAO'].isin(selic_dt['UltimoDiaMes'])]
    #bar()

    #selic.to_csv('tbl_selic.csv', index=False)
    # faz carga no banco de dados
    #print('--  carga da SELIC no SQLite\n')
    bar.text('carga da SELIC no SQLite')
    conn = sql.connect(cfg.bancodedados)
    selic.to_sql('tbl_selic', conn , schema=cfg.esquemaBD,index = False, if_exists='append')
    conn.close()
    bar()
    # ---------------------------------------------------------------------------


    # ---------------------------------------------------------------------------
    #
    #  Fechamento da Ibovespa
    assunto = ' Fechamentos da Ibovespa'
    #print('-'*(75-len(assunto))+assunto)
    #print('-- Inicio da importação da Ibovespa\n')
    bar.text('início da importação da Ibovespa')
    today = (dt.date.today() + dt.timedelta(1)).strftime('%Y-%m-%d')
    ibov = pd.DataFrame(YahooFinancials('^BVSP').get_historical_price_data('2016-12-01', today, 'daily')['^BVSP']['prices'])
    ibov = ibov.drop(columns=['date', 'close']).rename(columns={'formatted_date':'DT_COTACAO', 'adjclose':'FECHAMENTO', 'volume':'VOLUME'}).iloc[:,[5,0,1,2,3,4]]
    ibov = ibov.drop(columns=['high','low','open'])
    bar()
    #print('-- tratando datas...\n')
    bar.text('tratando datas')
    ibov['DT_COTACAO'] = pd.to_datetime(ibov['DT_COTACAO'])

    # cria coluna MES_COMPTC (para auxiliar mais adiante)
    #print('-- criando mês...\n')
    bar.text('criando mês...')
    ibov['MES_COTACAO']=ibov['DT_COTACAO'].apply(lambda x : x.replace(day=1))
    ibov=ibov[['DT_COTACAO','FECHAMENTO','VOLUME','MES_COTACAO']]
    bar()
    #
    # encontra a ultima data de cada mês
    ibov_dt = ibov.groupby(['MES_COTACAO']).agg({'DT_COTACAO':'max'})
    ibov_dt = ibov_dt.reset_index()
    meses = ibov_dt['MES_COTACAO']
    ibov_dt = ibov_dt.drop(['MES_COTACAO'], axis=1)
    ibov_dt.rename(columns={'DT_COTACAO':'UltimoDiaMes'}, inplace=True)
    #
    # exclui datas desnecessárias do dataframe
    #print('excluindo datas desnecessárias....')
    bar.text('excluindo datas desnecessárias...')
    ibov= ibov[ibov['DT_COTACAO'].isin(ibov_dt['UltimoDiaMes'])]
    bar()
    # faz carga no banco de dados
    #print('--  carga da Ibovespa no SQLite\n')
    bar.text('carga da Ibovespa no SQLite')
    conn = sql.connect(cfg.bancodedados)
    ibov.to_sql('tbl_ibovespa', conn , schema=cfg.esquemaBD,index = False, if_exists='append')
    conn.close()
    bar()

    #
    # ---------------------------------------------------------------------------


rodape='ETL encerrado'
print('-' * 75)
print(tt1+cfg.titulo+tt2)
print(' ' * (75-len(rodape))+rodape)
print('-' * 75)