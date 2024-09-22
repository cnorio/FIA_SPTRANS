#####################################################################################################
#
# Processamento dos arquivos JSON, gerados pela ingestao (pelo NIFI) da API Posicao da SPTRANS de uma
# determinada hora, para a geracao dos arquivos da camada Prata.
# A hora eh calculada a partir do numero de horas anteriores a atual que eh passado como argumento na 
# execucao.
#
# Comando de execucao:
# spark-submit SPTRANS_Transf_Prata_Posicao_V0.5.py <numero de horas anteriores a atual que ira ser 
#                                                    processada>
#
# Exemplo de execucao: 
# 1. Processa os arquivos JSON da hora anterior a atual
# spark-submit SPTRANS_Transf_Prata_Posicao_V0.5.py 1
#
# 1. Processa os arquivos JSON da hora atual
# spark-submit SPTRANS_Transf_Prata_Posicao_V0.5.pý 0


import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_utc_timestamp,split, substring, lit, col
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
# Leitura dos arquivos JSON ingeridos pelo NIFI da API Posicao, referentes a hora determinada pelo
# parâmetro de entrada
#

#Calcula da hora a ser processada baseada no parâmetro de entrada
GMT = zi.ZoneInfo('GMT')
LOCAL_TZ_STR='America/Sao_Paulo'
LOCAL_TZ = zi.ZoneInfo(LOCAL_TZ_STR)

dt_localtime=datetime.now(tz=LOCAL_TZ)
dt_lasthour= dt_localtime - timedelta(hours=hora_ant)

str_lasthour= dt_lasthour.strftime('%Y/%m/%d/%H')

str_data= dt_lasthour.strftime('%Y%m%d')


#Seta o path da camada bronze onde os arquivos da hora determinada serão lidos
bronze='s3a://bronze/API_SPTRANS_POSICAO_OK/' + str_lasthour + "/"

#Seta o path da camada prata onde serão persistidos os arquivos da camada prata
prata= 's3a://prata/POSICAO_PARQUET/' +  str_lasthour + "/"

print(bronze)
print(prata)

#Lê os arquivos JSON retornados pela API Posicao
df_bronze= spark.read.json(bronze)

#Exibe o schema 
df_bronze.printSchema()

#####################################################################################################
#
# Selecão das variáveis de interesse do JSON, normalizando as suas informacões e formatando-as em
# tabela
#

# Explodindo o array 'l' para criar uma nova linha para cada elemento do array
df_exploded = df_bronze.withColumn("l_exploded", explode("l"))

# Explodir o array 'vs' dentro do struct 'l_exploded'
df_vs_exploded = df_exploded.withColumn("vs_exploded", explode("l_exploded.vs"))

# Visualizar os campos de interesse
df_vs_exploded.select(
    col("hr").alias('hora_ref'),                  # hr : hora de referência da geração das infos
    col("vs_exploded.p").alias('cod_onibus'),     # p  : código do veículo    
    col("l_exploded.c").alias('let_cod_linha'),   # c  : código da linha no letreiro do ônibus    
    col("l_exploded.sl").alias('sentido_linha'),  # sl : sentido de operação da linha (1 do Term Principal para o Term Secundário - 2 do Term Secundário para o Term Principal)    
    col("l_exploded.lt0").alias('let_destino'),   # lt0: letreiro de destino da linha
    col("l_exploded.lt1").alias('let_origem'),    # lt1: letreiro de origem da linha
    from_utc_timestamp(split("vs_exploded.ta",'\+')[0],LOCAL_TZ_STR).alias('timestamp_pos'), #hora local da coleta das infos do ônibus
    col("vs_exploded.px").alias('latitude_pos'),  # px : latitude da posição do ônibus
    col("vs_exploded.py").alias('longitude_pos'), # py : longitude da posição do ônibus
    col("l_exploded.cl").alias('id_linha'),       # cl : código interno da linha
    col("l_exploded.qv").alias('qtde_onibus')     # qv : quantidade de ônibus localizados    
).show(5,truncate=False)

# Criar dataframe com os campos de interesse, normalizando as infos necessárias, para gravação na camada prata
df_norm= df_vs_exploded.select(
    col("hr").alias('hora_ref'),                  # hr : hora de referência da geração das infos
    col("vs_exploded.p").alias('cod_onibus'),     # p  : código do veículo    
    col("l_exploded.sl").alias('sentido_linha'),  # sl : sentido de operação da linha (1 do Term Principal para o Term Secundário - 2 do Term Secundário para o Term Principal)        
    col("l_exploded.c").alias('let_cod_linha'),   # c  : código da linha no letreiro do ônibus    
    col("l_exploded.lt0").alias('let_destino'),   # lt0: letreiro de destino da linha
    col("l_exploded.lt1").alias('let_origem'),    # lt1: letreiro de origem da linha
    from_utc_timestamp(split("vs_exploded.ta",'\+')[0],LOCAL_TZ_STR).alias('timestamp_pos'), #hora local da coleta das infos do ônibus
    col("vs_exploded.px").alias('latitude_pos'),  # px : latitude da posição do ônibus
    col("vs_exploded.py").alias('longitude_pos'), # py : longitude da posição do ônibus
    col("l_exploded.cl").alias('id_linha'),       # cl : código interno da linha
    col("l_exploded.qv").alias('qtde_onibus')     # qv : quantidade de ônibus localizados    
)

df_norm_data= df_norm.withColumn("data_ref", lit(str_data) )

df_result= df_norm_data.select(
    "data_ref",       # data : data de referencia da geracao das infos
    'hora_ref',       # hr : hora de referência da geração das infos
    'cod_onibus',     # p  : código do veículo    
    'sentido_linha',  # sl : sentido de operação da linha (1 do Term Principal para o Term Secundário - 2 do Term Secundário para o Term Principal)        
    'let_cod_linha',  # c  : código da linha no letreiro do ônibus    
    'let_destino',    # lt0: letreiro de destino da linha
    'let_origem',     # lt1: letreiro de origem da linha
    'timestamp_pos',  #hora local da coleta das infos do ônibus
    'latitude_pos',   # px : latitude da posição do ônibus
    'longitude_pos',  # py : longitude da posição do ônibus
    'id_linha',       # cl : código interno da linha
    'qtde_onibus'     # qv : quantidade de ônibus localizados  
    )

df_result.show(5)

#####################################################################################################
#
# Gravação do dataframe resultado do processamento na camada PRATA num path referente a última hora,
# em formato PARQUET.
# Obs: O processamento é feito para todos os arquivos da última hora, por isso utiliza-se o método
# overwrite para sobrepor os dados anteriores.
#

df_result.write.parquet(prata, mode='overwrite')

#####################################################################################################
#
# Fim do processamento