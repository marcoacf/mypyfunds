#!/usr/bin/env python
# coding: utf-8
#
#

def sqlCotas():
    consulta=("  SELECT CNPJ_FUNDO, "
            "\n       strftime('%Y', MES_COMPTC ) ANO, "  
            "\n       strftime('%m', MES_COMPTC ) MES, "  
            "\n       DATE(MES_COMPTC) MES_COMPTC,"  
            "\n       VL_QUOTA,"  
            "\n       VL_PATRIM_LIQ "  
            "\n  FROM tbl_cotas "  
            "\n ORDER BY CNPJ_FUNDO, MES_COMPTC ")
    return consulta

 
def sqlSelic():
    consulta=("SELECT  DATE(MES_COTACAO) MES_COTACAO,  "
            "\n        DATE(DT_COTACAO) DT_COTACAO,  "  
            "\n        strftime('%Y', MES_COTACAO ) ANO, "  
            "\n        strftime('%m', MES_COTACAO ) MES, "  
            "\n        TAXA "  
            "\n  FROM  tbl_selic "  
            "\n WHERE MES_COTACAO>='2016-12-01' "
            "\n ORDER BY MES_COTACAO, DT_COTACAO ASC; ")
    return consulta

def sqlIbov():
    consulta=("SELECT  DATE(MES_COTACAO) MES_COTACAO,  "
            "\n        DATE(DT_COTACAO) DT_COTACAO,  "  
            "\n        strftime('%Y', MES_COTACAO ) ANO, "  
            "\n        strftime('%m', MES_COTACAO ) MES, "  
            "\n        FECHAMENTO "  
            "\n  FROM  tbl_ibovespa "  
            "\n WHERE MES_COTACAO>='2016-12-01' "
            "\n ORDER BY MES_COTACAO, DT_COTACAO ASC; ")
    return consulta

def sqlDelCotas():
    # exclui os últimos 2 meses da base de cotas
    consulta=("delete from tbl_cotas where DT_COMPTC >=DATE('now','start of month','-2 month');")
    return consulta

def sqlMeses():
    consulta=("   SELECT date(MAX(DT_COMPTC)) dt_ult_pregao, "
            "\n          date(MAX(MES_COMPTC)) ult_pregao, "
            "\n          date(MAX(MES_COMPTC), '-1 months') mes_01, "
            "\n          date(MAX(MES_COMPTC), '-12 months')mes_12, "
            "\n          date(MAX(MES_COMPTC), '-24 months')mes_24, "
            "\n          date(MAX(MES_COMPTC), '-36 months')mes_36,  "
            "\n          strftime('%Y', MAX(MES_COMPTC)) ano0, "  
            "\n          strftime('%Y', date(MAX(MES_COMPTC), '-1 years')) ano1, "  
            "\n          strftime('%Y', date(MAX(MES_COMPTC), '-2 years')) ano2, "  
            "\n          strftime('%Y', date(MAX(MES_COMPTC), '-3 years')) ano3 "  
            "\n     FROM tbl_cotas ")
    return consulta

def sqlFundos():
    consulta=("SELECT "  
            "\n       CNPJ_FUNDO "  
            "\n  FROM tbl_cotas  /*"  
            "\n WHERE CNPJ_FUNDO "  
            "\n IN('00601692000123', "  
            "\n '01608573000165', "  
            "\n '04288966000127', "  
            "\n '07593972000186', "  
            "\n '03737219000166', "  
            "\n '00822055000187') */"  
            "\n GROUP BY CNPJ_FUNDO") 
    return consulta

def sqlDelCaracteristicas():
    # exclui os últimos 2 meses da base de cotações da SELIC
    consulta=("delete from tbl_caracteristicas;")
    return consulta

def sqlDelSelic():
    # exclui os últimos 2 meses da base de cotações da SELIC
    consulta=("delete from tbl_selic /* where DT_COTACAO >=DATE('now','start of month','-2 month') */ ;")
    return consulta

def sqlDelIbov():
    # exclui os últimos 2 meses da base da Ibovespa
    consulta=("delete from tbl_ibovespa /* where DT_COTACAO >=DATE('now','start of month','-2 month') */ ;")
    return consulta

def sqlDelRentabilidade():
    
    consulta=("delete from tbl_rentabilidade;")
    return consulta

def sqlDelSelicMes():    
    consulta=("delete from tbl_selic_mes;")
    return consulta
