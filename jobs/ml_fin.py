# Piopeline de Machine Learning, Engenharia de Dados, ETL e Banco de Dados 

import os
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import warnings
warnings.filterwarnings('ignore')

spark = SparkSession.builder \
    .appName("pipeline ml") \
    .getOrCreate()

# Configura o nível de log
spark.sparkContext.setLogLevel('ERROR')

df_sil = spark.read.csv('data/dados1_cap05.csv', header = True, inferSchema = True)
df_sil = df_sil.withColumn("Ano", year(df_sil["Data"]))
df_sil = df_sil.withColumn("Mes", month(df_sil["Data"]))
df_sil = df_sil.withColumn("Dia", dayofmonth(df_sil["Data"]))
df_sil = df_sil.withColumn("Hora", hour(df_sil["Data"]))

# Feature engineering
features = ['Valor', 'Ano', 'Mes', 'Dia', 'Hora']

assembler = VectorAssembler(inputCols=features, outputCol="features")
df_sil = assembler.transform(df_sil)

# Normalizar os dados
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
df_sil = scaler.fit(df_sil).transform(df_sil)

# Cria o modelo KMeans
kmeans = KMeans(featuresCol='scaledFeatures', k=3)  
modelo = kmeans.fit(df_sil)
df_resultado = modelo.transform(df_sil)
cluster_counts = df_resultado.groupBy('prediction').count()
cluster_counts.show()
df_resultado_clean = df_resultado.drop('features', 'scaledFeatures')
df_resultado_clean.show()

# Contagem total de linhas no DataFrame 
print(f"Total de linhas no DataFrame: {df_resultado_clean.count()}")

pandas_df = df_resultado_clean.toPandas()
output_dir = 'data/resultado_tarefa5'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Salvar o DataFrame do Pandas em uma nova tabela no SQLite
conn = sqlite3.connect("data/resultado_tarefa5/sil_database_ml.db")
pandas_df.to_sql("tb_clusters", conn, if_exists="replace", index=False)
conn.close()

spark.stop()

print("\nExecução do Job Concluída com Sucesso!\n")

