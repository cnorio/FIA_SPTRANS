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

O processamento no NiFi é composto com os seguintes processadores:
+ SPTRANS_TRIGGER_API: processador responsável por iniciar o pipeline de ingestão de dados no NiFi. Nele está configurado o scheduler, para iniciar o pipeline a cada 5 minutos (no mínimo).
+ SPTRANS_POST_AUTORIZAR: processador responsável por solicitar, através da API AUTENTICAR (POST), uma abertura de uma sessão através de uma KEY de autorização, obtida previamente no site da SPTRANS. As informações da sessão criada são devolvidas em um cookie armazenado no FileFlow do NiFi.
+ SPTRANS_GET_POSICAO: processador responsável por chamar a API POSICAO (GET), e receber os dados de localização de todos os ônibus em circulação na cidade de São Paulo em formato JSON.
+ SPTRANS_VALIDATE_FILESIZE: processador responsável por verificar se o acesso a API POSICAO foi um sucesso, baseado no tamanho em bytes dos dados retornados. Caso o tamanho seja maior que o configurado, esses dados são enviados para serem armazenados na camada bronze num path como sucesso. Caso contrário, são armazenados na camada bronze num path de erro.
+ SPTRANS_SAVE_OK_JSON_MINIO: processador responsável por armazenar os dados retornados da API POSICAO num path com sucesso.
+ SPTRANS_SAVE_NOK_JSON_MINIO: processador responsável por armazenar os dados retornados da API POSICAO num path de erro. 

Seguem abaixo os dados das API:
+ API Autorizar
  Método: Post
  URL: http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=<token>

+ API Posicao
  Método: Get
  URL: http://api.olhovivo.sptrans.com.br/v2.1/Posicao

