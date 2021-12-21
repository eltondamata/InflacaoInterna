#elton.mata@martins.com.br

import pandas as pd
from tabulate import tabulate
import sys
sys.path.insert(0, r'C:\oracle\dwh')
from OracleDWH import conn
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from pretty_html_table import build_table
from envia_mail import server
from email import encoders
pd.options.display.float_format = '{:,.2f}'.format

from dateutil.relativedelta import relativedelta
MESINI = int((pd.to_datetime("today") + relativedelta(months=-24)).strftime("%Y%m")) #Mes Anterior
MESFIM = int((pd.to_datetime("today") + relativedelta(months=-1)).strftime("%Y%m")) #Mes Anterior

#Consulta dimensao de produto
mysql = ("""
SELECT DISTINCT CODPRD AS CODMER
, DESPRD AS PRODUTO
, CODDIVFRN AS CODFRN
, DESDIVFRN AS FORNECEDOR
, NOMGRPECOFRN AS NOMGRPECOFRN
, CODGRPPRD AS CODGRPPRD
, DESGRPPRD AS GRUPO_PRODUTO
, CODCTGPRD AS CODCTGPRD
, DESCTGPRD AS CATEGORIA_PRODUTO
, CODSUBCTGPRD AS CODSUBCTGPRD
, DESSUBCTGPRD AS SUBCATEGORIA_PRODUTO
, DESCLLCMPATU AS CELULA
, DESDRTCLLATU AS DIRETORIA
FROM (
Select DISTINCT CODPRD, DESPRD, CODDIVFRN, DESDIVFRN, NOMGRPECOFRN, CODGRPPRD, DESGRPPRD, CODCTGPRD, DESCTGPRD, CODSUBCTGPRD, DESSUBCTGPRD, DESCLLCMPATU, DESDRTCLLATU, DATATURGT, ROW_NUMBER() OVER(PARTITION BY CODPRD ORDER BY DATATURGT DESC) AS RowNumber
from dwh.dimprd 
where CODSPRTIPPRD = 'PRD'
ORDER BY DATATURGT DESC
)
where RowNumber = 1
  """)
dimprd = pd.read_sql(mysql, con=conn)

#metricas para calculo da inflacao interna (export da tabela MRT.MOVMESALTNIVPCOMER)
mysql = (f"""     
SELECT A.ANOMESREF,
       A.CODMER, A.CODFILEMP, A.PSOMERTOTCMPMRT,  
       NVL(A.PERALTNIVPCOMER,0) * A.PSOMERTOTCMPMRT AS PESO_ITEM
FROM MRT.MOVMESALTNIVPCOMER A
WHERE A.PERALTNIVPCOMER BETWEEN -200 AND 200
  AND A.ANOMESREF between {MESINI} and {MESFIM}
  """)
df = pd.read_sql(mysql, con=conn)
conn.close()

df.fillna(0) #define zero para registros nulos
df = pd.merge(df, dimprd, how='inner', on=['CODMER']) #relaciona tabela metricas com dimensao produto

#Calculo Inflacao Total Empresa
dftot = df.query('PSOMERTOTCMPMRT>0').groupby(['ANOMESREF'])[['PSOMERTOTCMPMRT','PESO_ITEM']].sum()
dftot.eval('IND=PESO_ITEM/PSOMERTOTCMPMRT', inplace=True)
#dataset com inflacao mensal total dfcsv1
dfcsv1 = dftot.reset_index()
dfcsv1 = dfcsv1[['ANOMESREF', 'IND']]
dfcsv1.rename(columns={'IND':'TOTAL'}, inplace=True)
dftot['INDMES'] = dftot['IND']/100+1 #Indice Inflacao Mensal
dftot['INDACU'] = dftot['INDMES'].cumprod() #Indice Inflacao Acumulada
dftot['Inflacao12m'] = dftot['INDACU'].pct_change(12).fillna(0) #Percentual Inflacao acumulada 12 meses
dftot = dftot.iloc[:,-1:] * 100
dftot = dftot.query('Inflacao12m!=0')
dftot = dftot.reset_index()
dftot.insert(0, 'DIRETORIA', ' TOTAL')

#Calculo Inflacao Diretoria
dfdir = df.query('PSOMERTOTCMPMRT>0 and DIRETORIA!="MARKETING"').groupby(['DIRETORIA', 'ANOMESREF'])[['PSOMERTOTCMPMRT','PESO_ITEM']].sum()
dfdir.eval('IND=PESO_ITEM/PSOMERTOTCMPMRT', inplace=True) #Percentual Inflacao Mensal
#dataset com inflacao mensal diretoria dfcsv2
dfcsv2 = dfdir.groupby(['ANOMESREF', 'DIRETORIA'])[['IND']].sum()
dfcsv2 = dfcsv2.unstack(-1)
cols = dfcsv2.columns
cols.get_level_values(0)
cols.get_level_values(1)
l1 = cols.get_level_values(1)
l0 = cols.get_level_values(0)
names = [x[1] if x[1] else x[0] for x in zip(l0, l1)]
dfcsv2.columns = names
dfcsv2 = dfcsv2.reset_index()
dfcsv = pd.merge(dfcsv2, dfcsv1) #unifica dataset csv
dfdir['INDMES'] = dfdir['IND']/100+1 #Indice Inflacao Mensal
dfdir = dfdir.iloc[:,-1:] #Manter apenas indice a e última coluna com INDMES
dfdir = dfdir.unstack(-1) #Transpor linhas para coluna (mantem apenas uma dimensao na linha para calcular o indice acumulado 12 meses de cada diretoria)
dfdir = dfdir.cumprod(axis=1) #Multiplica linhas da coluna INDMES (Resultado=indice inflacao acumulada)
dfdir = dfdir.pct_change(axis=1, periods=12).fillna(0) #Percentual Inflacao acumulada 12 meses (Divide Indice Inflacao acumulada da linha atual pela linha de 12 meses antes)
dfdir = dfdir.stack() #Transpor as colunas mes para linha
dfdir.rename(columns={'INDMES':'Inflacao12m'}, inplace=True) #Corrige nome da coluna para Inflacao acumulada 12 meses
dfdir = dfdir.query('Inflacao12m!=0') #Filtra apenas as linhas que contem percentual Inflacao acumulada 12 meses
dfdir = dfdir * 100 #Multiplica o valor do indicador por 100
dfdir = dfdir.reset_index()

#Agrupa dados
#df_full = pd.concat([dfcel, dfdir, dftot]) #Unifica os datasets
df_full = pd.concat([dfdir, dftot]) #Unifica os datasets
df_full = df_full.set_index(['DIRETORIA', 'ANOMESREF']) #coloca as dimensoes no indice para transpor os meses para coluna
df_full = df_full.unstack().reset_index() #Transpoe os meses para coluna

#Ajusta nome das colunas
cols = df_full.columns
cols.get_level_values(0)
cols.get_level_values(1)
l1 = cols.get_level_values(1)
l0 = cols.get_level_values(0)
names = [x[1] if x[1] else x[0] for x in zip(l0, l1)]
df_full.columns = names
#print na tela
#print(tabulate(df_full, headers='keys', tablefmt='psql', floatfmt=',.2f', showindex=False, numalign='right'),'\n') #imprime o resultado final na tela
#Export arquivo.csv (Inflacao Mensal)
dfcsv.to_csv(r'InflacaoInterna_Mensal.csv', sep=";", encoding="iso-8859-1", decimal=",", float_format='%.8f', date_format='%d/%m/%Y', index=False)

#envia email
address_book = ['francisco.faria@martins.com.br', 'elisangela.teixeira@martins.com.br', 'edvania.silva@martins.com.br', 'leonardo.soraggi@martins.com.br']
sender = 'elton.mata@martins.com.br'
subject = "Inflação Interna"
tabela = build_table(df_full, 'blue_light', text_align='right')

body = f"""<html><body><p>Inflação Interna <i>(percentual acumulado em 12 meses findos no mês apreesntado)</i></p>
{tabela}
<p><i>(email programado para envio automático todo dia 2)</i></p>
</body></html>
"""
#anexar arquivo
file = "InflacaoInterna_Mensal.csv"
attachment = open(file,'rb')
obj = MIMEBase('application','octet-stream')
obj.set_payload((attachment).read())
encoders.encode_base64(obj)
obj.add_header('Content-Disposition',"attachment; filename= "+file)

msg = MIMEMultipart() 
msg['From'] = sender
msg['To'] = ','.join(address_book)
msg['Subject'] = subject
msg.attach(MIMEText(body, 'html'))
msg.attach(obj)
text=msg.as_string()
server.sendmail(sender,address_book, text)
server.quit()
