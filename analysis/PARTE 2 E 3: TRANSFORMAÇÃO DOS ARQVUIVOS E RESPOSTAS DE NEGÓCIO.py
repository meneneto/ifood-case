# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Continuação do desafio, uso de um worskpace do databricks premium para o uso do dbustils.fs.mount e copiar os arquivos do s3 para um diretório se tonando mais fácil manipular os arquivos.
# MAGIC

# COMMAND ----------

ACCESS_KEY = "AKIAS7HW2P6VXLPJCXH2"
SECRET_KEY = "OuHbXnm0ST5C56mlmDOVDwnXZQZX5oaCGXPH9gIe"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "meneneto-tlc"
MOUNT_NAME = "meneneto-tlc"

# COMMAND ----------

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

dbutils.fs.ls("/mnt/%s" % MOUNT_NAME + "/trip-data/" + "2023/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - EXPLORANDO OS ARQUIVOS

# COMMAND ----------

df_fhv = spark.read.parquet("dbfs:/mnt/meneneto-tlc/trip-data/2023/fhv_tripdata_2023-01.parquet")
display(df_fhv)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC analise estatisticas e descritivas das tabelas 

# COMMAND ----------

df_fhv.describe().show()

# COMMAND ----------

df_fhv.summary().show()

# COMMAND ----------

df_green = spark.read.parquet("dbfs:/mnt/meneneto-tlc/trip-data/2023/green_tripdata_2023-01.parquet")
display(df_green)

# COMMAND ----------

df_green.summary().show()

# COMMAND ----------

df_yellow = spark.read.parquet("dbfs:/mnt/meneneto-tlc/trip-data/2023/yellow_tripdata_2023-01.parquet")
display(df_yellow)


# COMMAND ----------

df_yellow.summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - são dfs com colunas diferentes e estatisticamente diferentes, para facilitar manipulação vou criar três views concentrando todos os meses para green, yellow e fhv; 
# MAGIC - para isso vai ser necessário movimentar para três pastas diferentes
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/mnt/%s" % MOUNT_NAME + "/trip-data/" + "2023/")

# COMMAND ----------

# DBTITLE 1,MOVIMENTANDO PARA TRÊS PASTAS
files = dbutils.fs.ls("/mnt/%s" % MOUNT_NAME + "/trip-data/" + "2023/")

for file in files:
    if file.name.endswith(".parquet") and not file.isDir():
        filename = file.name
        
        if filename.startswith("fhv"):
            folder = "fhv"
        elif filename.startswith("green"):
            folder = "green"
        elif filename.startswith("yellow"):
            folder = "yellow"
        else:
            continue  

        source = file.path
        dest = f"dbfs:/mnt/datalake/raw/{folder}/{filename}"
        print(f"Movendo {source} → {dest}")
        dbutils.fs.mv(source, dest)


# COMMAND ----------

# DBTITLE 1,VERIFICAÇÃO
dbutils.fs.ls('dbfs:/mnt/datalake/raw/fhv/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/datalake/raw/green/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/datalake/raw/yellow/')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC -  CRIAÇÃO DAS TRÊS VIEWS E  MONTANDO OS SCHEMAS DAS TABELAS 
# MAGIC

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/datalake/raw/fhv/fhv_tripdata_2023-10.parquet").printSchema()

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType, TimestampType
from pyspark.sql.functions import *
import pyspark.sql.functions as F

schema = StructType([
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("PUlocationID", LongType(), True),
        StructField("DOlocationID", LongType(), True),
        StructField("SR_Flag", StringType(), True), 
        StructField("Affiliated_base_number", StringType(), True)

])

df_f = spark.read.schema(schema).parquet("dbfs:/mnt/datalake/raw/fhv/").withColumn('date_partition', F.expr('date(pickup_datetime)'))

df_f = df_f.filter((year("pickup_datetime") == 2023) & (month("pickup_datetime") <= 5))

display(df_f)

# COMMAND ----------

# DBTITLE 1,criação da primeira view
df_f.createOrReplaceTempView('vw_fhv')

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO  DA VIEW GREEN UTILIZANDO AS COLUNAS NECESSÁRIAS
spark.read.parquet("dbfs:/mnt/datalake/raw/green/green_tripdata_2023-01.parquet").printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType, TimestampType
from pyspark.sql.functions import *
import pyspark.sql.functions as F

schema_green = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("total_amount", DoubleType(), True)
])


df_green = spark.read.option('spark.sql.parquet.enable.dictionary', 'false').schema(schema_green).parquet("dbfs:/mnt/datalake/raw/green/").withColumn('date_partition', F.expr('date(lpep_pickup_datetime)'))



df_g = df_green.filter((F.year("lpep_pickup_datetime" ) == 2023) & (F.month("lpep_pickup_datetime" ) <= 5))
display(df_g)

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/datalake/raw/yellow/yellow_tripdata_2023-01.parquet").printSchema()

# COMMAND ----------

# DBTITLE 1,SCHEMA YELLOW

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType, TimestampType
from pyspark.sql.functions import *
import pyspark.sql.functions as F

schema_yellow = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("total_amount", DoubleType(), True)
])


df_y = spark.read.schema(schema_yellow).parquet("dbfs:/mnt/datalake/raw/yellow/") \
                 .selectExpr(
                     'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'total_amount'
                 ).withColumn('date_partition', F.expr('date(tpep_pickup_datetime)'))

df_y = df_y.filter((year("tpep_pickup_datetime" ) == 2023) & (month("tpep_pickup_datetime" ) <= 5))

display(df_y)

# COMMAND ----------

df_y.createOrReplaceTempView('vw_yellow')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>  ANALISE DAS TABELAS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     *
# MAGIC FROM vw_fhv

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC      *
# MAGIC
# MAGIC FROM vw_green

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC      *
# MAGIC
# MAGIC FROM  vw_yellow

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - RESPONDENDO AS PERGUNTAS:
# MAGIC
# MAGIC 1. Qual a média de valor total (total\_amount) recebido em um mês
# MAGIC considerando todos os yellow táxis da frota?
# MAGIC 2. Qual a média de passageiros (passenger\_count) por cada hora do dia
# MAGIC que pegaram táxi no mês de maio considerando todos os táxis da
# MAGIC frota?

# COMMAND ----------

# DBTITLE 1,PERGUNTA 1
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     month(date_partition) AS mes
# MAGIC   , round(avg(total_amount),2) AS media
# MAGIC
# MAGIC FROM vw_yellow
# MAGIC
# MAGIC WHERE total_amount is not null
# MAGIC
# MAGIC
# MAGIC GROUP BY 1
# MAGIC
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# DBTITLE 1,PERGUNTA 2
# MAGIC %sql
# MAGIC SELECT
# MAGIC   HOUR(lpep_pickup_datetime) AS hora,
# MAGIC   ROUND(AVG(passenger_count), 2) AS media
# MAGIC FROM vw_green
# MAGIC WHERE YEAR(lpep_pickup_datetime) = 2023 AND MONTH(lpep_pickup_datetime) = 5 AND passenger_count IS NOT NULL
# MAGIC GROUP BY hora
# MAGIC ORDER BY hora;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - PROBLEMA: DIFICULDADE DE LEITURA FULL DOS PARQUET DEVIDO A INCOMPATIBILIDADE DE ALGUNS TIPOS DAS COLUNAS PARA OS ARQUIVOS GREEN E YELLOW SOLUÇÃO ANALISAR APENAS O PARQUET DA QUESTÃO

# COMMAND ----------

# DBTITLE 1,TABELA GREEN
from pyspark.sql import functions as F

df_g5 = spark.read.parquet("dbfs:/mnt/datalake/raw/green/green_tripdata_2023-05.parquet") \
                  .withColumn('date_partition', F.expr('date(lpep_pickup_datetime)')) \
                  .withColumn('hora', F.hour('lpep_pickup_datetime')) \
                  .selectExpr(
                      'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 'total_amount', 'hora'
                  )

avg_passenger_count =  df_g5.groupBy('hora') \
                           .agg(F.avg('passenger_count').alias('media_maio_green')) \
                           .orderBy('hora')

display(avg_passenger_count)

# COMMAND ----------

# DBTITLE 1,GRAFICO GREEN

from pyspark.sql import functions as F

df_g5 = spark.read.parquet("dbfs:/mnt/datalake/raw/green/green_tripdata_2023-05.parquet") \
                  .withColumn('date_partition', F.expr('date(lpep_pickup_datetime)')) \
                  .withColumn('hora', F.hour('lpep_pickup_datetime')) \
                  .selectExpr(
                      'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 'total_amount', 'hora'
                  )

avg_passenger_count =  df_g5.groupBy('hora') \
                           .agg(F.avg('passenger_count').alias('media_maio_green')) \
                           .orderBy('hora')

display(avg_passenger_count)

# COMMAND ----------

# DBTITLE 1,TABELA YELLOW


from pyspark.sql import functions as F

df_y5 = spark.read.parquet("dbfs:/mnt/datalake/raw/yellow/yellow_tripdata_2023-05.parquet") \
                  .withColumn('date_partition', F.expr('date(tpep_pickup_datetime)')) \
                  .withColumn('hora', F.hour('tpep_pickup_datetime')) \
                  .selectExpr(
                      'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'total_amount', 'hora'
                  )

avg_passenger_count = df_y5.groupBy('hora') \
                           .agg(F.avg('passenger_count').alias('media_maio_yellow')) \
                           .orderBy('hora')

display(avg_passenger_count)

# COMMAND ----------

# DBTITLE 1,GRAFICO YELLOW


from pyspark.sql import functions as F

df_y5 = spark.read.parquet("dbfs:/mnt/datalake/raw/yellow/yellow_tripdata_2023-05.parquet") \
                  .withColumn('date_partition', F.expr('date(tpep_pickup_datetime)')) \
                  .withColumn('hora', F.hour('tpep_pickup_datetime')) \
                  .selectExpr(
                      'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'total_amount', 'hora'
                  )

avg_passenger_count = df_y5.groupBy('hora') \
                           .agg(F.avg('passenger_count').alias('media_maio_yellow')) \
                           .orderBy('hora')

display(avg_passenger_count)
