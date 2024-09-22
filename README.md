# Projeto Desafio do Curso de Pos Graduação e MBA em Big Data - Data Engineering
# SPTRANS

## Membros
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

## Descrição
O objetivo deste projeto é acessar API Posicao disponibilizada pela SPTRANS para consultar informações sobre ônibus em circulação na cidade de São Paulo. Essas informações serão processadas para gerar indicadores para medir a qualidade do serviço prestado.

Os principais indicadores gerados serão:
+ Número médio por hora de ônibus em circulação por linha.
+ Número total de ônibus em circulação por linha a cada 5 minutos.
+ Lista horária com as linhas cuja quantidade de ônibus em circulação atingiu uma meta acordada.
+ Lista horária  com as linhas cuja quantidade de ônibus em circulação ficaram abaixo de uma meta acordada.

O armazenamento dessas informações será feito em um Data Lake com uma estrutura medalhão de 3 camadas:
+ bronze: dados obtidos dos acessos a API Posicao da SPTrans, sem nenhuma transformação.
+ prata:  informações obtidas através de processamentos da camada bronze, visando a normalização e enriquecimento dos dados.
+ ouro:   informações obtidas através de aplicações de regras de negócio aplicadas as informações da camada prata. Nesta camada encontram-se as informações dos indicadores citados acima.

O pipeline criado para a consulta a API Posicao e os processamentos para as gerações das informações das camadas prata e ouro seguirão uma cadência de no mínimo 5 minutos.

Os indicadores gerados serão exibidos em um Dashboard, cujas informações serão atualizadas conforme a cadência acima.






