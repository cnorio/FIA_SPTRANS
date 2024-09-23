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
+ ----Nomenclatura dos arquivos: YYYYMMDD_HH_\<nome gerado internamente pelo NIFI\>.json
  ![image](https://github.com/user-attachments/assets/2dcc1967-f889-4b43-9830-51232e243fd3)


+ Arquivos com erro:
+ ----Path: API_SPTRANS_POSICAO_NOK/YYYYMMDD_HH
+ ----Nomenclatura dos arquivos: YYYYMMDD_HH_\<nome gerado internamente pelo NIFI\>.NOK
  ![image](https://github.com/user-attachments/assets/77f894cd-47fa-452a-8414-cec850be4ac5)


Onde: YYYY: ano com 4 dígitos MM: Mês com dois dígitos  DD: dia com dois dígitos  HH: hora com dois dígitos
Essas informações são relativas a data/hora de recepção dos dados da API.

### 3.2. Processamento - SPARK

Os dados recebidos na camada bronze são processados no SPARK através dos 3 programas Python abaixo:
+ SPTRANS_Transf_Prata_Posicao_Parquet_Vx.x.py: programa Python que irá transformar os JSON da camada bronze em um formato de tabela, selecionando as colunas de interesse e armazenando-as em arquivos com formato Parquet na camada prata.
+ SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py: programa Python que, a partir das informações da camada prata, irá gerar as estatísticas de quantidade de ônibus por linha e armazená-las em arquivos em formato Parquet na camada ouro.
+ SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py:programa Python que, a partir das informações estatísticas da camada ouro e da tabela com a meta de quantidade de ônbius por linha (tabela armazenada no PostgreSQL), irá gerar as listas de linhas que atingiram ou não a meta estipulada.

Esses programas Python foram escritos a partir dos Jupiter Notebooks abaixo, que são mais facilmente desenvolvidos, testados e depurados:
+ Transf_Prata_Posicao_Parquet_Vx.y.ipynb => SPTRANS_Transf_Prata_Posicao_Parquet_Vx.y.py
+ Transf_Ouro_Posicao_Parquet_Vx.y.ipynb => SPTRANS_Transf_Ouro_Posicao_Parquet_Vx.y.py
+ Transf_Ouro_Meta_Onibus_Vx.y.ipynb => SPTRANS_Transf_Ouro_Meta_Onibus_Vx.y.py


