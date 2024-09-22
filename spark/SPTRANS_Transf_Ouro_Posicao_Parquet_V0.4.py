#####################################################################################################
#
# Geração de estatísticas gerais obtidas a partir dos arquivos da camada Prata, que contêm informações da
# API Posicao da SPTRANS. Essas estatísticas serão armazenadas na camada ouro
#
# Abaixo as estatisticas geradas:
# 1. Estatística com a quantidade total por linha de ônibus circulando a cada medição
# 2. Estatística com a quantidade total geral de ônibus circulando a cada medição
# 3. Estatística com a média de ônibus por linha circulando por hora
# 4. Estatística com a média geral de ônibus circulando por hora
#
# *Obs: a medição se refere a cada chamada da API Posicao
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_utc_timestamp,split, substring, sum, avg, max,filter, ceil

import psycopg2
from psycopg2 import sql

from datetime import datetime, timedelta
import zoneinfo as zi

import argparse

spark = SparkSession.builder.appName("FIA-Proj-SPTRANS").enableHiveSupport().getOrCreate()

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

str_lasthour= dt_lasthour.strftime('%Y/%m/%d/%H')

#Seta o path da camada prata onde foram persistidos as informacões de posição dos ônibus
prata= 's3a://prata/POSICAO_PARQUET/' +  str_lasthour + "/"
#prata= 's3a://prata/POSICAO_PARQUET/*'

#Seta o path da camada ouro onde serão persistidos a média de ônibus circulando por linha por hora
ouro_avg_linha= 's3a://ouro/MEDIA_ONIBUS_POR_LINHA/' +  str_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos o total médio de ônibus circulando por hora
ouro_avg_geral= 's3a://ouro/MEDIA_ONIBUS_GERAL/' +  str_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos o total por linha de onibus circulando em cada medição
ouro_total_linha= 's3a://ouro/TOTAL_ONIBUS_POR_LINHA/' +  str_lasthour + "/"

#Seta o path da camada ouro onde serão persistidos o total de ônibus circulando em cada medição
ouro_total_geral= 's3a://ouro/TOTAL_ONIBUS_GERAL/' +  str_lasthour + "/"

print(prata)
print(ouro_avg_linha)
print(ouro_avg_geral)
print(ouro_total_linha)
print(ouro_total_geral)

#Lê os arquivos da camada prata com as infos retornados pela API Posicao
df_prata= spark.read.parquet(prata)

#Exibe o schema 
df_prata.printSchema()

df_prata.count()

df_prata.show(5)

#####################################################################################################
#
# Cria dataframe com a quantidade de ônibus circulando por linha em cada medição
#

df_group_time=df_prata.groupby('data_ref','hora_ref',substring('hora_ref',0,2).alias('hora_id_ref'),'id_linha','sentido_linha','let_cod_linha','let_destino','let_origem').agg( max('qtde_onibus').alias('qtde_onibus'))
df_group_time.show(5)

#####################################################################################################
#
# Calcula a quantidade de ônibus por linha em circulação a cada medição
#

df_total_linha= df_group_time.select('data_ref','hora_ref','id_linha','sentido_linha','let_cod_linha','let_destino','let_origem','qtde_onibus')

df_total_linha.show(5)

#####################################################################################################
#
# Calcula a quantidade média por hora do número de ônibus por linha circulando
#

df_avg_hr_linha=df_group_time.groupby('data_ref','hora_id_ref','id_linha','sentido_linha','let_cod_linha','let_destino','let_origem').agg( ceil(avg('qtde_onibus')).alias('qtde_onibus'))

df_avg_hr_linha.show(5)

#####################################################################################################
#
# Calcula a quantidade total geral a cada medição, dos ônibus em circulação
#
df_total_geral_aux=df_group_time.groupby('data_ref','hora_id_ref','hora_ref').agg( sum('qtde_onibus').alias('qtde_onibus'))

df_total_geral_aux.show(5)

df_total_geral= df_total_geral_aux.select('data_ref','hora_ref','qtde_onibus')

df_total_geral.show(5)

#####################################################################################################
#
# Calcula a média geral por hora do número de ônibus em circulação
#
df_avg_geral= df_total_geral_aux.groupby('data_ref','hora_id_ref').agg( ceil(avg('qtde_onibus')).alias('qtde_onibus'))
df_avg_geral.show(5)

#####################################################################################################
#
# Gravação dos dataframe na camada Ouro em formato parquet:
# 1. A quantidade de ônibus por linha em circulação a cada medição
# 2. A quantidade total geral de ônibus em circulação a cada medição
# 3. A média de ônibus por linha circulando por hora
# 4. A média geral de ônibus circulando por hora
#
df_total_linha.write.parquet(ouro_total_linha, mode='overwrite')

df_total_geral.write.parquet(ouro_total_geral, mode='overwrite')

df_avg_hr_linha.write.parquet(ouro_avg_linha, mode='overwrite')

df_avg_geral.write.parquet(ouro_avg_geral, mode='overwrite')



#####################################################################################################
#
# Gravação dos dataframe na base de dados Postgres:
# 1. A quantidade de ônibus por linha em circulação a cada medição
# 2. A quantidade total geral de ônibus em circulação a cada medição
# 3. A média de ônibus por linha circulando por hora
# 4. A média geral de ônibus circulando por hora
#

#configuração das informações de conexão com a base de dados Postgres
url = "jdbc:postgresql://db:5432/dvdrental"

properties = {
"user": "admin",
"password": "admin",
"driver": "org.postgresql.Driver"
}

#gravação dos dataframes nas tabelas stages 
df_total_linha.write.jdbc( url=url, table='sptrans.total_onibus_por_linha_stage',mode="overwrite", properties= properties)

df_total_geral.write.jdbc( url=url, table='sptrans.total_onibus_geral_stage',mode="overwrite", properties= properties)

df_avg_hr_linha.write.jdbc( url=url, table='sptrans.media_onibus_por_linha_stage',mode="overwrite", properties= properties)

df_avg_geral.write.jdbc( url=url, table='sptrans.media_onibus_geral_stage',mode="overwrite", properties= properties)

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

# Upsert na tabela total_onibus_geral_por_linha com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.total_onibus_por_linha(data_ref, hora_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, qtde_onibus)
   SELECT data_ref, hora_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, qtde_onibus
   FROM sptrans.total_onibus_por_linha_stage
ON CONFLICT (data_ref, hora_ref, id_linha) 
DO UPDATE SET 
    sentido_linha = EXCLUDED.sentido_linha,
    let_cod_linha = EXCLUDED.let_cod_linha,
    let_destino = EXCLUDED.let_destino,
    let_origem = EXCLUDED.let_origem,
    qtde_onibus = EXCLUDED.qtde_onibus;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
#conn.commit()

# Upsert na tabela total_onibus_geral com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.total_onibus_geral(data_ref, hora_ref, qtde_onibus)
   SELECT data_ref, hora_ref, qtde_onibus
   FROM sptrans.total_onibus_geral_stage
ON CONFLICT (data_ref, hora_ref) 
DO UPDATE SET 
    qtde_onibus = EXCLUDED.qtde_onibus;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
#conn.commit()

# Upsert na tabela media_onibus_por_linha com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.media_onibus_por_linha(data_ref, hora_id_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, qtde_onibus)
   SELECT data_ref, hora_id_ref, id_linha, sentido_linha, let_cod_linha, let_destino, let_origem, qtde_onibus
   FROM sptrans.media_onibus_por_linha_stage
ON CONFLICT (data_ref, hora_id_ref, id_linha) 
DO UPDATE SET 
    sentido_linha = EXCLUDED.sentido_linha,
    let_cod_linha = EXCLUDED.let_cod_linha,
    let_destino = EXCLUDED.let_destino,
    let_origem = EXCLUDED.let_origem,
    qtde_onibus = EXCLUDED.qtde_onibus;
"""

# Executar a consulta
cur.execute(query)

# Commit das mudanças
#conn.commit()


# Upsert na tabela media_onibus_geral com as linhas da respectiva tabela stage
query = """
INSERT INTO sptrans.media_onibus_geral(data_ref, hora_id_ref, qtde_onibus)
   SELECT data_ref, hora_id_ref, qtde_onibus
   FROM sptrans.media_onibus_geral_stage
ON CONFLICT (data_ref, hora_id_ref) 
DO UPDATE SET 
    qtde_onibus = EXCLUDED.qtde_onibus;
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
