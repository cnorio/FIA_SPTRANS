# Projeto Desafio do Curso de Pos Graduação e MBA em Big Data - Data Engineering
# SPTRANS

## Equipe de Alunos
+ Celso Martins
+ Celso Norio Okuyama
+ Gislaine Nisi

## Professores
+ Fábio Jardim
+ Fernando Silva
+ Daniel
+ Rafael Negrão
+ Felipe

## Professores Orientadores
+ Dra Alessandra de Ávila Montini
+ Dr Adolpho Walter Pimazoni Canton

## 1. Objetivo
O objetivo deste projeto é a partir do acesso a API Posicao disponibilizada pela SPTRANS, para consultar informações sobre ônibus em circulação na cidade de São Paulo, gerar indicadores para medir a qualidade do serviço prestado.

Os principais indicadores a serem gerados são:
+ Número médio por hora de ônibus em circulação por linha.
+ Número total de ônibus em circulação por linha a cada 5 minutos.
+ Lista horária com as linhas cuja quantidade de ônibus em circulação atingiu uma meta acordada.
+ Lista horária  com as linhas cuja quantidade de ônibus em circulação ficaram abaixo de uma meta acordada.

## 2. Arquitetura da solução

A arquitetura da solução é um pipeline que envolverá os seguintes processos:
+ Ingestão: processo responsável pelo acesso a API e envio dessas informações para armazenamento.
+ Transformação: processo responsável por normalizar, enriquecer, aplicar regras de negócio nos dados ingeridos e envio dessas informações para armazenamento.
+ Visualização: processo responsável por exibir em dashboard as informações armazenadas que foram geradas a partir das regras de negócio.

O armazenamento será feito através de um Data Lake, com uma estrutura medalhão de 3 camadas:
+ bronze: armazenamento dos dados obtidos dos acessos a API Posicao da SPTrans, sem nenhuma transformação.
+ prata : armazenamento de informações normalizadas e enriquecidas a partir do processamentos da camada bronze.
+ ouro: : armazenamento das informações obtidas através de aplicações de regras de negócio nas informações da camada prata. Nesta camada estarão as informações dos indicadores citados acima.

Abaixo seguem as ferramentas que serão utilizadas:
+ Ingestão: NiFi
+ Transformação: Spark / Hive
+ Visualização: Grafana / PostgreSql
+ Armazenamento: Minio

## 3. Detalhes da Solução

### 3.1. Ingestão - NiFi

![image](https://github.com/user-attachments/assets/4d867816-25a6-44e7-8ca4-7e00a441546c)

#### 3.1.1. Processamento
O processamento no NiFi é composto com os seguintes processadores:
+ SPTRANS_TRIGGER_API: processador responsável por iniciar o pipeline de ingestão de dados no NiFi. Nele está configurado o scheduler, para iniciar o pipeline a cada 5 minutos (no mínimo).
+ SPTRANS_POST_AUTORIZAR: processador responsável por solicitar, através da API AUTENTICAR (POST), uma abertura de uma sessão através de uma KEY de autorização, obtida previamente no site da SPTRANS. As informações da sessão criada são devolvidas em um cookie armazenado no FileFlow do NiFi.
+ SPTRANS_GET_POSICAO: processador responsável por chamar a API POSICAO (GET), e receber os dados de localização de todos os ônibus em circulação na cidade de São Paulo em formato JSON.
+ SPTRANS_VALIDATE_FILESIZE: processador responsável por verificar se o acesso a API POSICAO foi um sucesso, baseado no tamanho em bytes dos dados retornados. Caso o tamanho seja maior que o configurado, esses dados são enviados para serem armazenados em arquivos na camada bronze num path como sucesso. Caso contrário, são armazenados em arquivos na camada bronze num path de erro. O formato dos arquivos armazenados é JSON.
+ SPTRANS_SAVE_OK_JSON_MINIO: processador responsável por armazenar os dados retornados da API POSICAO num path com sucesso.
+ SPTRANS_SAVE_NOK_JSON_MINIO: processador responsável por armazenar os dados retornados da API POSICAO num path de erro. 

Seguem abaixo os dados das API:
+ API: Autorizar
  ++Método: Post
  ++URL: http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=<token>

+ API: Posicao
  ++Método: Get
  ++URL: http://api.olhovivo.sptrans.com.br/v2.1/Posicao

#### 3.1.2. Descrição do JSON
![image](https://github.com/user-attachments/assets/2d809f81-6b7e-4fd0-a8a5-3dc665151e9e)

Abaixo a descrição dos campos em JSON retornados pela API Posicao
+ [string]hr: Horário de referência da geração das informações 
+ [{}]l: Relação de linhas localizadas onde: 
+ ----[string]c: Letreiro completo 
+ ----[int]cl: Código identificador da linha 
+ ----[int]sl: Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal 
+ ----[string]lt0: Letreiro de destino da linha
+ ----[string]lt1: Letreiro de origem da linha 
+ ----[int]qv: Quantidade de veículos localizados 
+ ----[{}]vs: Relação de veículos localizados, onde: 
+ --------[int]p: Prefixo do veículo 
+ --------[bool]a: Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência 
+ --------[string]ta: Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601 
+ --------[double]py: Informação de latitude da localização do veículo 
+ --------[double]px: Informação de longitude da localização do veículo

#### 3.1.3. Armazenamento Camada Bronze
O armazenamento na camada bronze é feito no Minio:
+ Bucket: bronze

+ Arquivos com recepção OK:
+ ----Path: API_SPTRANS_POSICAO_OK/YYYY/MM/DD/HH
+ ----Layout: Ver metadados item 4.1.1
+ ----Nomenclatura dos arquivos: YYYYMMDD_HH_\<nome gerado internamente pelo NIFI\>.json
  ![image](https://github.com/user-attachments/assets/2dcc1967-f889-4b43-9830-51232e243fd3)


+ Arquivos com erro:
+ ----Path: API_SPTRANS_POSICAO_NOK/YYYYMMDD_HH
+ ----Nomenclatura dos arquivos: YYYYMMDD_HH_\<nome gerado internamente pelo NIFI\>.NOK
  ![image](https://github.com/user-attachments/assets/77f894cd-47fa-452a-8414-cec850be4ac5)


Onde: YYYY: ano com 4 dígitos MM: Mês com dois dígitos  DD: dia com dois dígitos  HH: hora com dois dígitos
Essas informações são relativas a data/hora de recepção dos dados da API.

### 3.2. Processamento - SPARK

Os dados recebidos na camada bronze são processados no SPARK através dos 3 programas Python abaixo, os quais geraram as informações para as camadas prata e ouro:
+ SPTRANS_Transf_Prata_Posicao_Parquet_Vx.x.py
+ SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py:
+ SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.pyestipulada.

Esses programas Python foram escritos a partir dos Jupiter Notebooks abaixo, que são mais facilmente desenvolvidos, testados e depurados:
+ Transf_Prata_Posicao_Parquet_Vx.y.ipynb => SPTRANS_Transf_Prata_Posicao_Parquet_Vx.y.py
+ Transf_Ouro_Posicao_Parquet_Vx.y.ipynb => SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py
+ Transf_Ouro_Meta_Onibus_Vx.y.ipynb => SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py

#### 3.2.1 Programa SPTRANS_Transf_Prata_Posicao_Parquet_Vx.x.py
+ Descrição: Programa Python que irá transformar os arquivos em JSON da camada bronze em um formato de tabela, selecionando as colunas de interesse e armazenando-as em arquivos com formato Parquet na camada prata. Os arquivos processados são referentes a uma determinada hora, anteriores a hora atual.
+ Execução: spark-submit SPTRANS_Transf_Prata_Posicao_Parquet_Vx.x.py <horas anteriores a atual>  onde: <horas anteriores a atual>: a hora a ser processada é a hora atual menos a quantidade especificada nesse parâmetro.
+ Entrada: Arquivos em formato JSON que se encontram na camada bronze no path: API_SPTRANS_POSICAO_OK/YYYY/MM/DD/HH onde HH é a hora a ser processada
+ Saída: Arquivos em formato Parquet, com os dados dos ônibus em formato de tabela, que ficarão armazenadas na camada prata.
+ ----Bucket: prata
+ ----Path: POSICAO_PARQUET/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
![image](https://github.com/user-attachments/assets/a95635f4-241e-4984-981b-6fde70934e5a)  
+ ----Layout: Ver Metadados item 4.2.1


#### 3.2.2 Programa SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py
+ Descrição: programa Python que, a partir das informações da camada prata, irá gerar as estatísticas de quantidade de ônibus por linha e armazená-las em arquivos em formato Parquet na camada ouro. Os arquivos processados são referentes a uma determinada hora, anteriores a hora atual.
+ Execução: spark-submit SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py <horas anteriores a atual>  onde: <horas anteriores a atual>: a hora a ser processada é a hora atual menos a quantidade especificada nesse parâmetro.
+ Entrada: Arquivos em formato Parquet que se encontram na camada prata no path: POSICAO_PARQUET/YYYY/MM/DD/HH onde HH é a hora a ser processada (ver saída do item 3.2.1)
+ Saída: Arquivos em formato Parquet, com as estatística de quantidade de ônibus, que ficarão armazenadas na camada ouro.
+ ---- Saída 1: Estatística da média da quantidade de ônibus por hora por linha
+ --------Bucket: ouro
+ --------Path: MEDIA_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
  
![image](https://github.com/user-attachments/assets/1467c349-8c34-4cbd-b05a-ebae2d032afb)

+ --------Layout:
+ ------------campo1:
+ ------------campo2:

+ ---- Saída 2: Estatística da quantidade total de ônibus por linha na data/hora de geração dos dados
+ --------Bucket: ouro
+ --------Path: TOTAL_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
  
![image](https://github.com/user-attachments/assets/3703051f-7ff6-4046-9e8b-5b00db36ded1)

+ --------Layout:
+ ------------campo1:
+ ------------campo2:

  
#### 3.2.3 Programa SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py
+ Descrição: programa Python que, a partir das informações estatísticas da camada ouro e da tabela com a meta de quantidade de ônbius por linha (tabela armazenada no PostgreSQL), irá gerar as listas de linhas que atingiram ou não a meta estipulada. Os arquivos processados são referentes a uma determinada hora, anteriores a hora atual.
+ Execução: spark-submit SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py <horas anteriores a atual>  onde: <horas anteriores a atual>: a hora a ser processada é a hora atual menos a quantidade especificada nesse parâmetro.
+ Entrada: Arquivos em formato Parquet que se encontram na camada ouro no path: MEDIA_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde HH é a hora a ser processada.
+ -------: Tabela meta_onibus_por_linha que se encontra armazenada no PostgreSQL.
+ Saída: Arquivos em formato Parquet, com as estatística das linhas que atingiram ou não a meta estipulada de quantidade de ônibus na hora a ser processada.

#### 3.2.4 Scheduler de execução
+ Os programas SPTRANS_Transf_Prata_Posicao_Parquet_Vx.y.py e SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py são executados através da crontab com frequência igual ou menor a 5 minutos.
+ O programa SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py é executado também pela crontab com frequência horária, visto que a validação com a tabela meta_onibus_por_linha precisa ser feito com a média horária.


### 3.3. Visualização - Grafana
Os gráficos exibidos no dashboard do Grafana são baseados nos dados das tabelas abaixo da camada ouro:
+ sptrans.linhas_abiaxo_meta
+ sptrans.total_linhas_abaixo_meta
+ sptrans.linhas_atingida_meta
+ sptrans.total_linhas_atingida_meta
+ sptrans.media_onibus_por_linha
+ sptrans.total_onibus_geral


## 4. Metadados

### 4.1. Camada Bronze

#### 4.1.1. API_SPTRANS_POSICAO_OK

+ Path: API_SPTRANS_POSICAO_OK/YYYY/MM/DD/HH
+ Nomenclatura dos arquivos: YYYYMMDD_HH_\<nome gerado internamente pelo NIFI\>.json
  ![image](https://github.com/user-attachments/assets/2dcc1967-f889-4b43-9830-51232e243fd3)

+Layout
+ hr: string : Horário de referência da geração das informações 
+ l:[{}]: Relação de linhas localizadas onde: 
+ ----c: string: Letreiro completo 
+ ----cl: int: Código identificador da linha 
+ ----sl: int: Sentido de operação onde 1 significa de Terminal Principal. para Secundário e 2 de Terminal Secundário para Principal 
+ ----lt0: string: Letreiro de destino da linha
+ ----lt1: string: Letreiro de origem da linha 
+ ----qv:int : Quantidade de veículos localizados 
+ ----vs:[{}] :Relação de veículos localizados, onde: 
+ --------p: int:  Prefixo do veículo 
+ --------a: bool: Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência 
+ --------ta: string: Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601 
+ --------py: double:Informação de latitude da localização do veículo 
+ --------px: double:Informação de longitude da localização do veículo

![image](https://github.com/user-attachments/assets/2d809f81-6b7e-4fd0-a8a5-3dc665151e9e)

### 4.2. Camada Prata

#### 4.2.1. POSICAO_PARQUET

+ Path: POSICAO_PARQUET/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada

![image](https://github.com/user-attachments/assets/a95635f4-241e-4984-981b-6fde70934e5a)

+ Layout:
+ ----data_ref:  string    : data de referencia da geracao dos dados
+ ----hora_ref:  string    : hora de referência da geração dos dados
+ ----cod_onibus:    long  : código do veículo    
+ ----sentido_linha: long  : sentido de operação da linha (1 do Term Principal para o Term Secundário - 2 do Term Secundário para o Term Principal)        
+ ----let_cod_linha: string  : código da linha no letreiro do ônibus    
+ ----let_destino:   string  : letreiro de destino da linha
+ ----let_origem:    string  : letreiro de origem da linha
+ ----timestamp_pos: timestamp : data/hora local da coleta das infos do ônibus
+ ----latitude_pos:  double : latitude da posição do ônibus
+ ----longitude_pos: double : longitude da posição do ônibus
+ ----id_linha:      long   : código interno da linha
+ ----qtde_onibus:   long   : quantidade de ônibus localizados

![image](https://github.com/user-attachments/assets/c270a958-2ec5-4331-817b-7b3f1e3e1f3c)

### 4.3. Camada Ouro

#### 4.3.1 MEDIA_ONIBUS_POR_LINHA

#### 4.3.2 TOTAL_ONIBUS_POR_LINHA

#### 4.3.3 MEDIA_ONIBUS_GERAL

#### 4.3.4 TOTAL_ONIBUS_GERAL

#### 4.3.5 LINHAS_ABAIXO_META

#### 4.3.6 TOTAL_LINHAS_ABAIXO_META

#### 4.3.7 LINHAS_ATINGIDA_META

#### 4.3.8 TOTAL_LINHAS_ATINGIDA_META

### 4.4. Tabelas PostgreSql



