#!/usr/bin/env python
# coding: utf-8

#

# endereço do repositório de CVM para a posição dos fundos
url_cotas = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'

# endereço do repositório da CMV com o cadastro de cada fundo
url_carac = 'http://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/'

# extensão dos arquivos a serem importados
ext = 'csv'

# endereço do repositório da SELIC
# https://www.bcb.gov.br/htms/SELIC/SELICdiarios.asp?frame=1
url_selic ='http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json'

# nome do banco de dados SQLite
bancodedados = 'investdb.db'
esquemaBD = 'investdb'

#
titulo = ' Projeto ETL Fundos '


