# Projeto Desafio do Curso de Pos Graduação e MBA em Big Data - Data Engineering
# TEMA: API SPTRANS

## Equipe de Alunos
+ Celso Martins
+ Celso Norio Okuyama
+ Gislene Nisi

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

![Arquitetura_20240923_02](https://github.com/user-attachments/assets/78b196cf-2f0a-4fba-9ef2-49f15c2b3369)


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
+ Entrada: Arquivos em formato JSON que se encontram na camada bronze no path: API_SPTRANS_POSICAO_OK/YYYY/MM/DD/HH onde HH é a hora a ser processada. Ver Metadados item 4.1.1
+ Saída: Arquivos em formato Parquet, com os dados dos ônibus retornados pela API Posicao em formato de tabela, armazenados na camada prata.
+ ----Bucket: prata
+ ----Path: POSICAO_PARQUET/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ ----Layout: Ver Metadados item 4.2.1
  
![image](https://github.com/user-attachments/assets/a95635f4-241e-4984-981b-6fde70934e5a)  

![image](https://github.com/user-attachments/assets/c270a958-2ec5-4331-817b-7b3f1e3e1f3c)

#### 3.2.2 Programa SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py
+ Descrição: programa Python que, a partir das informações da camada prata, irá gerar as estatísticas de quantidade de ônibus por linha e armazená-las em arquivos em formato Parquet na camada ouro. Os arquivos processados são referentes a uma determinada hora, anteriores a hora atual.
+ Execução: spark-submit SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py <horas anteriores a atual>  onde: <horas anteriores a atual>: a hora a ser processada é a hora atual menos a quantidade especificada nesse parâmetro.
+ Entrada: Arquivos em formato Parquet que se encontram na camada prata no path: POSICAO_PARQUET/YYYY/MM/DD/HH onde HH é a hora a ser processada (ver saída do item 3.2.1)
+ Saída: Arquivos em formato Parquet, com as estatística de quantidade de ônibus, que ficarão armazenadas na camada ouro.
+ ---- Saída 1: Estatística da média da quantidade de ônibus por hora por linha
+ --------Bucket: ouro
+ --------Path: MEDIA_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.1
  
![image](https://github.com/user-attachments/assets/1467c349-8c34-4cbd-b05a-ebae2d032afb)

![image](https://github.com/user-attachments/assets/dcba776d-b102-4bae-9ad7-626dc0e56589)

+ ---- Saída 2: Estatística da média da quantidade de ônibus por hora
+ --------Bucket: ouro
+ --------Path: MEDIA_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.2
  
![image](https://github.com/user-attachments/assets/d728e524-58a7-4736-a067-08011cd72e22)

![image](https://github.com/user-attachments/assets/4591c170-d8ae-46a6-bdcf-399449ab53f0)
  
+ ---- Saída 3: Estatística da quantidade total de ônibus por linha na data/hora de geração dos dados
+ --------Bucket: ouro
+ --------Path: TOTAL_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.3
  
![image](https://github.com/user-attachments/assets/3703051f-7ff6-4046-9e8b-5b00db36ded1)

![image](https://github.com/user-attachments/assets/29f295e8-73f9-4f99-8f70-ada6e67895e1)

+ ---- Saída 4: Estatística da quantidade total de ônibus por data/hora de geração dos dados
+ --------Bucket: ouro
+ --------Path: TOTAL_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.4
  
![image](https://github.com/user-attachments/assets/e4b28329-e0bf-47bd-8137-39f19d012cfc)  

![image](https://github.com/user-attachments/assets/640ad75f-22be-4cf7-ad83-e7ca21dbcd52)

+ ---- Saída 5: As mesmas informações armazenadas na camada ouro nas saídas 1, 2, 3 e 4 também são armazenadas respectivamente nas tabelas PostgreSQL:
+ --------Tabela: sptrans.media_onibus_por_linha (ver Metadados item 4.4.2)
+ --------Tabela: sptrans.media_onibus_geral (ver Metadados item 4.4.3)
+ --------Tabela: sptrans.total_onibus_por_linha (ver Metadados item 4.4.4)
+ --------Tabela: sptrans.total_onibus_por_linha (ver Metadados item 4.4.5)
  
#### 3.2.3 Programa SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py
+ Descrição: programa Python que, a partir das informações estatísticas da camada ouro e da tabela com a meta de quantidade de ônbius por linha (tabela armazenada no PostgreSQL), irá gerar as listas de linhas que atingiram ou não a meta estipulada. Os arquivos processados são referentes a uma determinada hora, anteriores a hora atual.
+ Execução: spark-submit SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py <horas anteriores a atual>  onde: <horas anteriores a atual>: a hora a ser processada é a hora atual menos a quantidade especificada nesse parâmetro.
+ Entrada: Arquivos com a quantidade média de ônibus por linha da hora a ser processada e tabela do PostgreSQL com a meta da quantidade de ônibus por linha.
+ ---- Entrada1: Arquivos em formato Parquet que se encontram na camada ouro no path: MEDIA_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde HH é a hora a ser processada. Ver Metadados item 4.3.1.
+ ---- Entrada2: Tabela meta_onibus_por_linha que se encontra armazenada no PostgreSQL. Ver Metadados item 4.4.1.
+ Saída: Arquivos em formato Parquet, com as estatística das linhas que atingiram ou não a meta estipulada de quantidade de ônibus na hora a ser processada.
+ ---- Saída 1: Lista das linhas de ônibus cujas médias horárias ficaram abaixo da meta da quantidade de ônibus.
+ --------Bucket: ouro
+ --------Path: LINHAS_ABAIXO_META/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.5

![image](https://github.com/user-attachments/assets/290f9739-f7e4-46f4-a8d4-b0e3c68dbe45)

![image](https://github.com/user-attachments/assets/695aa1a1-8c7c-4c19-b339-cf0b3ca90055)

+ ---- Saída 2: Estatística da quantidade total de linhas de ônibus cujas médias horárias ficaram abaixo da meta da quantidade de ônibus em uma data/hora.
+ --------Bucket: ouro
+ --------Path: TOTAL_LINHAS_ABAIXO_META/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.6

![image](https://github.com/user-attachments/assets/6a8b0be1-256f-4108-88eb-1c260d8b60e8)

![image](https://github.com/user-attachments/assets/160c5fa3-a767-4c35-8a54-bf0487c21178)

+ ---- Saída 3: Lista das linhas de ônibus cujas médias horárias atingiram a meta da quantidade de ônibus.
+ --------Bucket: ouro
+ --------Path: LINHAS_ATINGIDA_META/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.7
  
![image](https://github.com/user-attachments/assets/6fc98a56-47e9-405f-a507-de142d462ab1)

![image](https://github.com/user-attachments/assets/78780098-0ab5-451b-be6d-6d675baf9f90)

+ ---- Saída 4: Estatística da quantidade total de linhas de ônibus cujas médias horárias atingiram a meta da quantidade de ônibus em uma data/hora.
+ --------Bucket: ouro
+ --------Path: TOTAL_LINHAS_ATINGIDA_META/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada
+ --------Layout: Ver Metadados item 4.3.8

![image](https://github.com/user-attachments/assets/8f15a973-ffa0-4649-ba6b-38c748baa632)

![image](https://github.com/user-attachments/assets/d954b10d-e093-4097-a117-796a4392d213)

+ ---- Saída 5: As mesmas informações armazenadas na camada ouro nas saídas 1, 2, 3 e 4 também são armazenadas respectivamente nas tabelas PostgreSQL:
+ --------Tabela: sptrans.linhas_abaixo_meta (ver Metadados item 4.4.8)
+ --------Tabela: sptrans.total_linhas_abaixo_meta (ver Metadados item 4.4.9)
+ --------Tabela: sptrans.linhas_atingida_meta (ver Metadados item 4.4.6)
+ --------Tabela: sptrans.total_linhas_atingida_meta (ver Metadados item 4.4.7)
  
#### 3.2.4 Scheduler de execução
+ Os programas SPTRANS_Transf_Prata_Posicao_Parquet_Vx.y.py e SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py são executados através da crontab com frequência igual ou menor a 5 minutos.
+ O programa SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py é executado também pela crontab com frequência horária, visto que a validação com a tabela meta_onibus_por_linha precisa ser feito com a média horária.


### 3.3. Visualização - Grafana
Os gráficos exibidos no dashboard do Grafana são baseados nos dados das tabelas abaixo do PostgreSql:
+ sptrans.linhas_abaixo_meta (ver descrição no item 4.4.8)
+ sptrans.total_linhas_abaixo_meta (ver descrição no item 4.4.9)
+ sptrans.total_onibus_geral (ver descrição no item 4.4.5)
+ sptrans.total_onibus_por_linha (ver descrição no item 4.4.4)
+ sptrans.media_onibus_geral (ver descrição no item 4.3.3)
+ sptrans.media_onibus_por_linha (ver descrição no item 4.4.2)
  
![image](https://github.com/user-attachments/assets/5017dd7d-a086-48c5-a50f-2e0e919b9a0d)

![image](https://github.com/user-attachments/assets/6948dc57-bd0d-4f1d-9dbc-dece08832893)


## 4. Metadados

### 4.1. Camada Bronze

#### 4.1.1. API_SPTRANS_POSICAO_OK
+ Descrição: Arquivos com os dados dos ônibus retornados pela API Posicao em formato de Json, sem nenhum tratamento.
+ Path: API_SPTRANS_POSICAO_OK/YYYY/MM/DD/HH
+ Nomenclatura dos arquivos: YYYYMMDD_HH_\<nome gerado internamente pelo NIFI\>.json
  ![image](https://github.com/user-attachments/assets/2dcc1967-f889-4b43-9830-51232e243fd3)

+Layout
+ hr: string : Horário de referência da geração das informações 
+ l:[{}]: Relação de linhas localizadas onde: 
+ ----c: string: Letreiro completo 
+ ----cl: int: Código identificador da linha 
+ ----sl: int: Sentido de operação onde 1 significa de Term Principal. para Secundário e 2 de Term Secundário para Principal 
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
+ Descrição: Arquivos em formato Parquet, com os dados retornados pela API Posicao selecionados e normalizados em formato de tabela.
+ Path: POSICAO_PARQUET/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada

![image](https://github.com/user-attachments/assets/a95635f4-241e-4984-981b-6fde70934e5a)

+ Layout:
+ ----data_ref:  string    : data de referencia da geracao dos dados
+ ----hora_ref:  string    : hora de referência da geração dos dados
+ ----cod_onibus:    long  : código do veículo    
+ ----sentido_linha: long  : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)        
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
+ Descrição: Estatística da quantidade média de ônibus em circulação por hora por linha.
+ Path: MEDIA_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.
  
![image](https://github.com/user-attachments/assets/1467c349-8c34-4cbd-b05a-ebae2d032afb)

+ Layout:
+ ----data_ref:      string  : data de referencia do cálculo da média
+ ----hora_id_ref:   string  : hora de referência do cálculo da média
+ ----id_linha:      long    : código interno da linha
+ ----sentido_linha: long    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: string  : código da linha no letreiro do ônibus    
+ ----let_destino:   string  : letreiro de destino da linha
+ ----let_origem:    string  : letreiro de origem da linha
+ ----qtde_onibus:   long    : quantidade média de ônibus da linha

![image](https://github.com/user-attachments/assets/dcba776d-b102-4bae-9ad7-626dc0e56589)

#### 4.3.2 MEDIA_ONIBUS_GERAL
+ Descrição: Estatística da quantidade média de ônibus em circulação por hora.
+ Path: MEDIAL_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.

![image](https://github.com/user-attachments/assets/d728e524-58a7-4736-a067-08011cd72e22)

+ Layout:
+ ----data_ref:      string  : data de referencia do cálculo da média
+ ----hora_id_ref:   string  : hora de referência do cálculo da média
+ ----qtde_onibus:   long    : quantidade média de ônibus

![image](https://github.com/user-attachments/assets/4591c170-d8ae-46a6-bdcf-399449ab53f0)


#### 4.3.3 TOTAL_ONIBUS_POR_LINHA
+ Descrição: Estatística da quantidade total de ônibus por linha em circulação na data/hora de geração dos dados.
+ Path: TOTAL_ONIBUS_POR_LINHA/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.
  
![image](https://github.com/user-attachments/assets/3703051f-7ff6-4046-9e8b-5b00db36ded1)

+ Layout:
+ ----data_ref:      string  : data de referencia da geração dos dados
+ ----hora_ref:  string      : hora de referência da geração dos dados
+ ----id_linha:      long    : código interno da linha
+ ----sentido_linha: long    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: string  : código da linha no letreiro do ônibus    
+ ----let_destino:   string  : letreiro de destino da linha
+ ----let_origem:    string  : letreiro de origem da linha
+ ----qtde_onibus:   long    : quantidade total de ônibus da linha

![image](https://github.com/user-attachments/assets/29f295e8-73f9-4f99-8f70-ada6e67895e1)


#### 4.3.4 TOTAL_ONIBUS_GERAL
+ Descrição: Estatística da quantidade total de ônibus em circulação na data/hora de geração dos dados.
+ Path: TOTAL_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.

![image](https://github.com/user-attachments/assets/e4b28329-e0bf-47bd-8137-39f19d012cfc)

+ Layout:
+ ----data_ref:      string  : data de referencia da geração dos dados
+ ----hora_ref:  string      : hora de referência da geração dos dados
+ ----qtde_onibus:   long    : quantidade total de ônibus em circulação

![image](https://github.com/user-attachments/assets/640ad75f-22be-4cf7-ad83-e7ca21dbcd52)

  
#### 4.3.5 LINHAS_ABAIXO_META
+ Descrição: Lista das linhas de ônibus cujas médias horárias ficaram abaixo da meta da quantidade de ônibus.
+ Path: LINHAS_ABAIXO_META/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.

![image](https://github.com/user-attachments/assets/290f9739-f7e4-46f4-a8d4-b0e3c68dbe45)

+ Layout:
+ ----data_ref:      string  : data da verificação da meta
+ ----hora_id_ref:   string  : hora da verificação da meta
+ ----id_linha:      long    : código interno da linha
+ ----sentido_linha: long    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: string  : código da linha no letreiro do ônibus    
+ ----let_destino:   string  : letreiro de destino da linha
+ ----let_origem:    string  : letreiro de origem da linha
+ ----meta_qtde_onibus:   long   : meta da quantidade de ônibus da linha para a hora de verificação
+ ----avg_qtde_onibus:   long    : média da quantidade de ônibus da linha para a hora de verificação

![image](https://github.com/user-attachments/assets/695aa1a1-8c7c-4c19-b339-cf0b3ca90055)


#### 4.3.6 TOTAL_LINHAS_ABAIXO_META
+ Descrição: Estatística da quantidade total de linhas de ônibus cujas médias horárias ficaram abaixo da meta da quantidade de ônibus em uma data/hora.
+ Path: TOTAL_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.

![image](https://github.com/user-attachments/assets/6a8b0be1-256f-4108-88eb-1c260d8b60e8)


+ Layout:
+ ----data_ref:      string  : data da verificação da meta
+ ----hora_id_ref:   string  : hora da verificação da meta
+ ----qtde_linhas:   long   : quantidade de de linhas que ficaram abaixo da meta para a hora de verificação

![image](https://github.com/user-attachments/assets/160c5fa3-a767-4c35-8a54-bf0487c21178)

  
#### 4.3.7 LINHAS_ATINGIDA_META
+ Descrição: Lista das linhas de ônibus cujas médias horárias atingiram a meta da quantidade de ônibus.
+ Path: TOTAL_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.

![image](https://github.com/user-attachments/assets/6fc98a56-47e9-405f-a507-de142d462ab1)


+ Layout:
+ ----data_ref:      string  : data da verificação da meta
+ ----hora_id_ref:   string  : hora da verificação da meta
+ ----id_linha:      long    : código interno da linha
+ ----sentido_linha: long    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: string  : código da linha no letreiro do ônibus    
+ ----let_destino:   string  : letreiro de destino da linha
+ ----let_origem:    string  : letreiro de origem da linha
+ ----meta_qtde_onibus:   long   : meta da quantidade de ônibus da linha para a hora de verificação
+ ----avg_qtde_onibus:   long    : média da quantidade de ônibus da linha para a hora de verificação


![image](https://github.com/user-attachments/assets/78780098-0ab5-451b-be6d-6d675baf9f90)

  
#### 4.3.8 TOTAL_LINHAS_ATINGIDA_META
+ Descrição: Estatística da quantidade total de linhas de ônibus cujas médias horárias atingiram a meta da quantidade de ônibus em uma data/hora.
+ Path: TOTAL_ONIBUS_GERAL/YYYY/MM/DD/HH onde YYYY: ano MM: Mês DD: Dia e HH: Hora - São referentes a data/hora processada.

![image](https://github.com/user-attachments/assets/8f15a973-ffa0-4649-ba6b-38c748baa632)

+ Layout:
+ ----data_ref:      string  : data da verificação da meta
+ ----hora_id_ref:   string  : hora da verificação da meta
+ ----qtde_linhas:   long   : quantidade de de linhas que atingiram a meta para a hora de verificação

![image](https://github.com/user-attachments/assets/d954b10d-e093-4097-a117-796a4392d213)

  
### 4.4. Tabelas PostgreSql

#### 4.4.1. Tabela meta_onibus_por_linha
+ Schema: sptrans
+ Descrição: Tabela com a meta de quantidade de ônibus por hora de cada uma das linhas de ônibus
+ Layout:
+ ----hora_id_ref:   text  : hora de referência da meta
+ ----id_linha:      int8    : código interno da linha
+ ----sentido_linha: int8    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: text  : código da linha no letreiro do ônibus    
+ ----let_destino:   text  : letreiro de destino da linha
+ ----let_origem:    text  : letreiro de origem da linha
+ ----meta_qtde_onibus:   int8  : meta da quantidade de ônibus da linha

  
#### 4.4.2. Tabela media_onibus_por_linha
+ Schema: sptrans
+ Descrição: tabela com a média horária de ônibus por linha referente a uma data/hora
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo da média
+ ----hora_id_ref:   text  : hora de referência do cálculo da média
+ ----id_linha:      int8    : código interno da linha
+ ----sentido_linha: int8    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: text  : código da linha no letreiro do ônibus    
+ ----let_destino:   text  : letreiro de destino da linha
+ ----let_origem:    text  : letreiro de origem da linha
+ ----qtde_onibus:   int8  : quantidade média de ônibus da linha

#### 4.4.3. Tabela media_onibus_geral
+ Schema: sptrans
+ Descrição:  tabela com a média horária geral de ônibus circulando em uma data/hora
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo da média
+ ----hora_id_ref:   text  : hora de referência do cálculo da média
+ ----qtde_onibus:   int8  : quantidade média geral de ônibus

#### 4.4.4. Tabela total_onibus_por_linha
+ Schema: sptrans
+ Descrição: tabela com o total de ônibus por linha circulando na data/hora em que foram obtidas as informacões através da  API Posicao
+ Layout:
+ ----data_ref:      text  : data de referência da geração dos dados
+ ----hora_ref:   text  : hora de referência da geração dos dados
+ ----id_linha:      int8    : código interno da linha
+ ----sentido_linha: int8    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: text  : código da linha no letreiro do ônibus    
+ ----let_destino:   text  : letreiro de destino da linha
+ ----let_origem:    text  : letreiro de origem da linha
+ ----qtde_onibus:   int8  : quantidade total de ônibus da linha

#### 4.4.5. Tabela total_onibus_geral
+ Schema: sptrans
+ Descrição:  tabela com o total de ônibus circulando na data/hora em que foram obtidas as informações através da  API Posicao
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo do total
+ ----hora_id_ref:   text  : hora de referência do cálculo do total
+ ----qtde_onibus:   int8  : quantidade total de ônibus

#### 4.4.6. Tabela linhas_atingida_meta
+ Schema: sptrans
+ Descrição: tabela com a lista das linhas de ônibus que atingiram a meta da média de quantidade de ônibus circulando em uma data/hora.
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo da média
+ ----hora_id_ref:   text  : hora de referência do cálculo da média
+ ----id_linha:      int8    : código interno da linha
+ ----sentido_linha: int8    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: text  : código da linha no letreiro do ônibus    
+ ----let_destino:   text  : letreiro de destino da linha
+ ----let_origem:    text  : letreiro de origem da linha
+ ----meta_qtde_onibus:   int8  : meta da quantidade de ônibus da linha
+ ----avg_qtde_onibus:   int8  : média da quantidade de ônibus da linha
  
#### 4.4.7. Tabela total_linhas_atingida_meta
+ Schema: sptrans
+ Descrição:   tabela com o total de linhas de ônibus que atingiram a meta da média de quantidade de ônibus circulando em uma data/hora.
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo do total
+ ----hora_id_ref:   text  : hora de referência do cálculo do total
+ ----qtde_linhas:   int8  : quantidade total de linhas

#### 4.4.8. Tabela linhas_abaixo_meta
+ Schema: sptrans
+ Descrição: tabela com a lista das linhas de ônibus que ficaram abaixo da meta da média de quantidade de ônibus circulando em uma data/hora.
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo da média
+ ----hora_id_ref:   text  : hora de referência do cálculo da média
+ ----id_linha:      int8    : código interno da linha
+ ----sentido_linha: int8    : sentido de operação da linha (1 do Term Principal para o Secundário - 2 do Term Secundário para o Principal)
+ ----let_cod_linha: text  : código da linha no letreiro do ônibus    
+ ----let_destino:   text  : letreiro de destino da linha
+ ----let_origem:    text  : letreiro de origem da linha
+ ----meta_qtde_onibus:   int8  : meta da quantidade de ônibus da linha
+ ----avg_qtde_onibus:   int8  : média da quantidade de ônibus da linha
  
#### 4.4.9. Tabela total_linhas_abaixo_meta
+ Schema: sptrans
+ Descrição:   tabela com o total de linhas de ônibus que ficaram abaixo da meta da média de quantidade de ônibus circulando em uma data/hora.
+ Layout:
+ ----data_ref:      text  : data de referência do cálculo do total
+ ----hora_id_ref:   text  : hora de referência do cálculo do total
+ ----qtde_linhas:   int8  : quantidade total de linhas
