#####################################################################################################
#
# Verifica se a media horária de ônibus em circulação por linha atingiu a meta horária 
# estabelecida
#
# Abaixo as informações geradas:
# 1. Lista de linhas que atingiram a meta estabelecida
# 2. Lista de linhas que estão abaixo da meta estabelecida
# 3. Total de linhas que atingiram a meta estabelecida
# 4. Total de linhas que estão abaixo da meta estabelecida
#
# *Obs: a meta estabelecida encontra-se na tabela META_ONIBUS_POR_LINHA
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_utc_timestamp,split, substring, sum, avg, max,filter, ceil, col, count

import psycopg2
from psycopg2 import sql

from datetime import datetime, timedelta
import zoneinfo as zi

import argparse

spark = SparkSession.builder.appName("FIA-Proj-SPTRANS-Ouro-Meta").enableHiveSupport().getOrCreate()

#####################################################################################################
#
# Obtem a quantidade de horas anteriores a atual passada como parametro de execucao.
#
parser= argparse.ArgumentParser()

parser.add_argument("hora", help="qtde de horas anteriores a ser processada", type=int )
args= parser.parse_args()
hora_ant= args.hora

#####################################################################################################
#
# Leitura dos arquivos da camada Prata relativos a uma determinada hora com as infos da API Posicao 
#

#Calcula a hora a ser processada
GMT = zi.ZoneInfo('GMT')
LOCAL_TZ_STR='America/Sao_Paulo'
LOCAL_TZ = zi.ZoneInfo(LOCAL_TZ_STR)

dt_localtime=datetime.now(tz=LOCAL_TZ)
dt_lasthour= dt_localtime - timedelta(hours=hora_ant)

str_dt_lasthour= dt_lasthour.strftime('%Y/%m/%d/%H')
str_lasthour= dt_lasthour.strftime('%H')
str_date= dt_lasthour.strftime('%Y%m%d')

#Seta o path da camada prata onde foram persistidos as informacões de posição dos ônibus
prata= 's3a://prata/POSICAO_PARQUET/' +  str_lasthour + "/"
#prata= 's3a://prata/POSICAO_PARQUET/*'

#####################################################################################################
#
# Seta o path da camada ouro onde estão as informações da média de ônibus em circulação da hora a
# ser verificada. Também são setados os paths onde serão armazenados as informações abaixo:
#
# 1. Lista de linhas que atingiram a meta estabelecida
# 2. Lista de linhas que estão abaixo da meta estabelecida
# 3. Total de linhas que atingiram a meta estabelecida
# 4. Total de linhas que estão abaixo da meta estabelecida
#

#Seta o path da camada ouro onde foram persistidos as informacões de média de ônibus em circulação
#por linha da hora a ser verificada
ouro_avg_linha= 's3a://ouro/MEDIA_ONIBUS_POR_LINHA/' +  str_dt_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos a lista das linhas que atingiram a meta
#estabelecida
ouro_lista_linhas_ok= 's3a://ouro/LINHAS_ATINGIDA_META/' +  str_dt_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos a lista das linhas que não atingiram
#a meta estabelecida
ouro_lista_linhas_abaixo= 's3a://ouro/LINHAS_ABAIXO_META/' +  str_dt_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos o total de linhas que atingiram  
#a meta estabelecida 
ouro_total_linhas_ok= 's3a://ouro/TOTAL_LINHAS_ATINGIDA_META/' +  str_dt_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos o total de linhas que estão abaixo da meta
#estabelecida 
ouro_total_linhas_abaixo= 's3a://ouro/TOTAL_LINHAS_ABAIXO_META/' +  str_dt_lasthour + "/"

print(ouro_avg_linha)
print(ouro_lista_linhas_ok)
print(ouro_lista_linhas_abaixo)
print(ouro_total_linhas_ok)
print(ouro_total_linhas_abaixo)

#####################################################################################################
#
# Configuração das informações de conexão com a base de dados Postgres
#
url = "jdbc:postgresql://db:5432/dvdrental"

properties = {
"user": "admin",
"password": "admin",
"driver": "org.postgresql.Driver"
}

#####################################################################################################
#
# Leitura da camada ouro com as informações estatísticas da quantidade de ônibus em circulação 
# na data/hora calculada
#
df_avg_linha= spark.read.parquet(ouro_avg_linha)

#Exibe o schema 
df_avg_linha.printSchema()

df_avg_linha.count()

df_avg_linha.show(5)

# Seleciona as colunas da hora, código da linha e qtde de ônibus
df_avg_linha_sel=  df_avg_linha.select(
    col("data_ref"),
    col("hora_id_ref").alias('avg_hora_id_ref'),
    col("id_linha").alias('avg_id_linha'),
    col("qtde_onibus").alias('avg_qtde_onibus'))

df_avg_linha_sel.printSchema()

df_avg_linha_sel.show(5)

#####################################################################################################
#
# Lê a tabela do PostgreSql com a meta horária de ônibus em circulação por linha
#
df_meta_linha= spark.read.jdbc( url=url, table='sptrans.meta_onibus_por_linha',properties= properties)

#Exibe o schema 
df_meta_linha.printSchema()

df_meta_linha.count()

df_meta_linha.show(5)

df_meta_hora= df_meta_linha.where(col("hora_id_ref") == str_lasthour )

df_meta_hora.show(5)

#####################################################################################################
#
# Realiza o left outer join entre as informações da meta e a média de ônibus em circulação
#

df_valida_qtde = df_meta_hora \
    .join(df_avg_linha_sel, ( df_meta_hora['hora_id_ref'] == df_avg_linha_sel['avg_hora_id_ref'] ) \
                              & ( df_meta_hora['id_linha'] == df_avg_linha_sel['avg_id_linha'] ) , 'left' ) \
    .drop( 'avg_hora_id_ref', 'avg_id_linha') \
    .fillna( {'avg_qtde_onibus':0,'data_ref': str_date} ) \
    .select( col("data_ref"), col("hora_id_ref"), col("id_linha"), col("sentido_linha"), col("let_cod_linha"), col("let_destino"), col("let_origem"), \
             col("meta_qtde_onibus"), col("avg_qtde_onibus") )

df_valida_qtde.show(5)

df_valida_qtde.where(col("avg_qtde_onibus") == 0).count()

#####################################################################################################
#
# Seleciona as linhas de ônibus que possuem um valor maior ou igual a meta estabelecida
#

df_lista_linhas_ok= df_valida_qtde.where( col("avg_qtde_onibus") >= col("meta_qtde_onibus") )

df_lista_linhas_ok.show(5)

#####################################################################################################
#
# Calcula a quantidade de linhas de ônibus que atingiram a meta
#

df_total_linhas_ok= df_lista_linhas_ok.groupby('data_ref','hora_id_ref').agg(count('sentido_linha').alias('qtde_linhas'))

df_total_linhas_ok.show()

#####################################################################################################
#
# Seleciona as linhas de ônibus que ficaram abaixo da meta
#

df_lista_linhas_abaixo= df_valida_qtde.where( col("avg_qtde_onibus") < col("meta_qtde_onibus") )

df_lista_linhas_abaixo.show(5)

#####################################################################################################
#
# Calcula a quantidade de linhas de ônibus que ficaram abaixo da meta do contrato
# de ônibus do contrato
#

df_total_linhas_abaixo= df_lista_linhas_abaixo.groupby('data_ref','hora_id_ref').agg(count('sentido_linha').alias('qtde_linhas'))

df_total_linhas_abaixo.show()

#####################################################################################################
#
# Gravação dos dataframe na camada Ouro em formato parquet:
# 1. Lista de linhas que atingiram a meta estabelecida
# 2. Lista de linhas que estão abaixo da meta estabelecida
# 3. Total de linhas que atingiram a meta estabelecida
# 4. Total de linhas que estão abaixo da meta estabelecida

df_lista_linhas_ok.write.parquet(ouro_lista_linhas_ok, mode='overwrite')

df_total_linhas_ok.write.parquet(ouro_total_linhas_ok, mode='overwrite')

df_lista_linhas_abaixo.write.parquet(ouro_lista_linhas_abaixo, mode='overwrite')

df_total_linhas_abaixo.write.parquet(ouro_total_linhas_abaixo, mode='overwrite')

#####################################################################################################
#
# Gravação dos dataframe na base de dados Postgres:
# 1. Lista de linhas que atingiram a meta estabelecida
# 2. Lista de linhas que estão abaixo da meta estabelecida
# 3. Total de linhas que atingiram a meta estabelecida
# 4. Total de linhas que estão abaixo da meta estabelecida
#

df_lista_linhas_ok.write.jdbc( url=url, table='sptrans.linhas_atingida_meta_stage',mode="overwrite", properties= properties)

df_total_linhas_ok.write.jdbc( url=url, table='sptrans.total_linhas_atingida_meta_stage',mode="overwrite", properties= properties)

df_lista_linhas_abaixo.write.jdbc( url=url, table='sptrans.linhas_abaixo_meta_stage',mode="overwrite", properties= properties)

df_total_linhas_abaixo.write.jdbc( url=url, table='sptrans.total_linhas_abaixo_meta_stage',mode="overwrite", properties= properties)

##################################################################
#
# upsert dos dados das stages nas tabelas fatos
#

#conexão na base de dados Postgres
conn = psycopg2.connect(
    dbname="dvdrental",
    user="admin",
    password="admin",
    host="db",
    port="5432"
)

# Criar um cursor
cur = conn.cursor()

# Upsert na tabela linhas_atingida_meta com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.linhas_atingida_meta(data_ref, hora_id_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, meta_qtde_onibus, avg_qtde_onibus)
   SELECT data_ref, hora_id_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, meta_qtde_onibus, avg_qtde_onibus
   FROM sptrans.linhas_atingida_meta_stage
ON CONFLICT (data_ref, hora_id_ref, id_linha) 
DO UPDATE SET 
    sentido_linha = EXCLUDED.sentido_linha,
    let_cod_linha = EXCLUDED.let_cod_linha,
    let_destino = EXCLUDED.let_destino,
    let_origem = EXCLUDED.let_origem,
    meta_qtde_onibus = EXCLUDED.meta_qtde_onibus,
    avg_qtde_onibus = EXCLUDED.avg_qtde_onibus;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
#conn.commit()

# Upsert na tabela total_linhas_atingida_meta com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.total_linhas_atingida_meta(data_ref, hora_id_ref, qtde_linhas)
   SELECT data_ref, hora_id_ref, qtde_linhas
   FROM sptrans.total_linhas_atingida_meta_stage
ON CONFLICT (data_ref, hora_id_ref) 
DO UPDATE SET 
    qtde_linhas = EXCLUDED.qtde_linhas;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
#conn.commit()

# Upsert na tabela linhas_abaixo_meta com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.linhas_abaixo_meta(data_ref, hora_id_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, meta_qtde_onibus, avg_qtde_onibus)
   SELECT data_ref, hora_id_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, meta_qtde_onibus, avg_qtde_onibus
   FROM sptrans.linhas_abaixo_meta_stage
ON CONFLICT (data_ref, hora_id_ref, id_linha) 
DO UPDATE SET 
    sentido_linha = EXCLUDED.sentido_linha,
    let_cod_linha = EXCLUDED.let_cod_linha,
    let_destino = EXCLUDED.let_destino,
    let_origem = EXCLUDED.let_origem,
    meta_qtde_onibus = EXCLUDED.meta_qtde_onibus,
    avg_qtde_onibus = EXCLUDED.avg_qtde_onibus;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
#conn.commit()

# Upsert na tabela total_linhas_abaixo_meta com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.total_linhas_abaixo_meta(data_ref, hora_id_ref, qtde_linhas)
   SELECT data_ref, hora_id_ref, qtde_linhas
   FROM sptrans.total_linhas_abaixo_meta_stage
ON CONFLICT (data_ref, hora_id_ref) 
DO UPDATE SET 
    qtde_linhas = EXCLUDED.qtde_linhas;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
conn.commit()

# Fechar o cursor e a conexão
cur.close()
conn.close()

#####################################################################################################
#
# Fim do processamento
