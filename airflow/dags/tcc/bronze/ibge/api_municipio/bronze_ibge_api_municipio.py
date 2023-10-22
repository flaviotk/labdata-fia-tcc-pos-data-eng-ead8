import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType

#-------------------------------

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
print('@@@&P&@@@@@@@@@@&P&@@@@@@@@@@@@@@@&P&@@@@@@@@@@@GB@@@@@@@@@@@@@@@@@&&@@#?????????YJ????J????????7G@@')
print('@@G. B@@@@@@@@@5  #@@@@@@@@@@@@@@@B .G@@@@@@@@@@~ ^@@@@@@@@@@@@@@@@?G@@7~@@@@@@@@!~@@@@.@@@@@@@@@:@@')
print('@@Y  #@G?~!?77BJ  7!~?G@@@@@@@G?~!!  Y@BJ~~??75P.  !?&BJ~~??75@@@@@?P@@7!@@@5????^.@@@@!B@@A:A@@G:@@')
print('@@Y  B~  !?^  YY  ^?!  ~&@@@&^  !?:  5J  ^?~  ~J.  :J?  ~?~  ~@@@@@?G@@7!@@@@@@FTK.@@@@.&@@@@@@@G:@@')
print('@@Y  P  ^@@G  YJ  G@&^  #G5##  ~@@P  Y: .&@&. ~@~ .&&. .&@#. !@@@@@?P@@7!@@@^~JJJ:?@@@@^G@@A:A@@G:@@')
print('@@Y  B5. .:.  YJ  .:. .PB  .&Y. .:.  5B: .:.  ~@Y   5B: .:.  ~@@@@@YG@@?^@@@.5FFF?^@@@@.@@@A.A@@@:@@')
print('@@#PG&@&BPGBBB&&BB#GPB&@@BP#@@&BPGBBB&@&#GPBBB#@@#GP#@&BPPBBB#@@@@@@@@@&Y?J?5&TTT&Y?JJ???JJ??JJJJB@@')
print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Definição de Schema para Processamento da Extração através da API de Municipio do IBGE ============')
print('============!============!============!============!============!============!============!============!============')

schema = StructType([
         StructField("id", LongType(), True),
         StructField("nome", StringType(), True),
         StructField("microrregiao", StructType([
             StructField("id", LongType(), True),
             StructField("nome", StringType(), True),
             StructField("mesorregiao", StructType([
                 StructField("id", LongType(), True),
                 StructField("nome", StringType(), True),
                 StructField("UF", StructType([
                     StructField("id", IntegerType(), True),
                     StructField("sigla", StringType(), True),
                     StructField("nome", StringType(), True),
                     StructField("regiao", StructType([
                         StructField("id", IntegerType(), True),
                         StructField("sigla", StringType(), True),
                         StructField("nome", StringType(), True)
                     ]))
                 ]))
             ]))
         ])),
         StructField("regiao-imediata", StructType([
             StructField("id", LongType(), True),
             StructField("nome", StringType(), True),
             StructField("regiao-intermediaria", StructType([
                 StructField("id", LongType(), True),
                 StructField("nome", StringType(), True),
                 StructField("UF", StructType([
                     StructField("id", IntegerType(), True),
                     StructField("sigla", StringType(), True),
                     StructField("nome", StringType(), True),
                     StructField("regiao", StructType([
                         StructField("id", IntegerType(), True),
                         StructField("sigla", StringType(), True),
                         StructField("nome", StringType(), True)
                     ]))
                 ]))
             ]))
         ]))
     ])
print('==========> schema:', schema)
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Leitura da API de Municipios do IBGE extração do JSON ============')
print('============!============!============!============!============!============!============!============!============')

url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
response = requests.get(url)
j_resp = json.loads(response.text)

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregamento do Dataframe df_municipio com os Dados do Objeto JSON ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio = spark.createDataFrame(data=j_resp, schema=schema)

df_municipio.show(5, False)
print('============!============!============!============!============!============!============!============!============')

df_municipio.printSchema()
print('============!============!============!============!============!============!============!============!============')


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Ajuste de Nomes de Campos com traço para permitir leitura do Trino ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio2 = df_municipio \
    .withColumnRenamed("regiao-imediata", "regiao_imediata") \
    .withColumn("regiao_imediata", 
                col("regiao_imediata").cast("struct<id:int,nome:string,regiao_intermediaria:struct<id:int,nome:string,UF:struct<id:int,sigla:string,nome:string,regiao:struct<id:int,sigla:string,nome:string>>>>"))

# Selecionando apenas os campos desejados
df_municipio3 = df_municipio2.select("id", "nome", "microrregiao", "regiao_imediata")


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando Dados na CAMADA BRONZE  ============')
print('============!============!============!============!============!============!============!============!============')

(df_municipio3
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://bronze/ibge/municipio')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA BRONZE a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio_evd = (spark.read.format('parquet')
                   .load('s3a://bronze/ibge/municipio'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA BONZE ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet:", df_municipio_evd.count())
print('============!============!============!============!============!============!============!============!============')

df_municipio_evd.show(5, False)
print('============!============!============!============!============!============!============!============!============')

df_municipio_evd.printSchema()
print('============!============!============!============!============!============!============!============!============')
