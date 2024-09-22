echo "Run Camada Prata"
date
docker exec spark-master spark-submit /home/user/SPTRANS_Transf_Prata_Posicao_Parquet_V0.7.py 1 
date

echo "Run Camada Ouro"
date
docker exec spark-master spark-submit /home/user/SPTRANS_Transf_Ouro_Posicao_Parquet_V0.4.py 1 
date
