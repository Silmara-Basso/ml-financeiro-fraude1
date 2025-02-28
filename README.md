# ml-financeiro-fraude1
Analise de anomalias para fraude com KMeans

## Criar e Inicializar o Cluster
docker compose -f docker-compose.yml up -d --scale spark-worker=3

### Spark Master
http://localhost:9090

### History Server
http://localhost:18080



### Execução do script python 
O script ml-fin.py e o csv devem estar na pasta jobs e dados compartilhadas na criação do cluster em Docker

### arquivo de dados.csv
Eu gerei um arquivo dados.csv com 1.000.000 de registros para testar, não disponibilizei pelo tamanho
colunas esperadas ID_Transacao,Data,Valor,ID_Cliente,Anomalia

docker exec sil-spark-master spark-submit --deploy-mode client ./apps/ml-fin.py