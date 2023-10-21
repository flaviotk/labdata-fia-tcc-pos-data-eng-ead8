import requests
import json
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import SparkSession

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
print('============ Definição de Schema para Processamento da Extração através da API de Estado do IBGE ============')
print('============!============!============!============!============!============!============!============!============')

schema = StructType([
                     StructField('id', StringType(), True),
                     StructField('nome', StringType(), True),
                     StructField('regiao', StructType([
                                           StructField('nome', StringType(), True),
                                           StructField('sigla', StringType(), True),
                                           StructField('id', StringType(), True)
                                          ]), True),
                     StructField('sigla', StringType(), True)
                  ])
print('==========> schema:', schema)
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Leitura da API de Estados do IBGE extração do JSON ============')
print('============!============!============!============!============!============!============!============!============')

url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
response = requests.get(url)
j_resp = json.loads(response.text)

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregamento do Dataframe df_estado com os Dados do Objeto JSON ============')
print('============!============!============!============!============!============!============!============!============')

df_estado = spark.createDataFrame(data=j_resp, schema=schema)

df_estado.show(5, False)
print('============!============!============!============!============!============!============!============!============')

df_estado.printSchema()
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando Dados na CAMADA BRONZE  ============')
print('============!============!============!============!============!============!============!============!============')

(df_estado
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://bronze/ibge/estado')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA BRONZE a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

df_estado_evd = (spark.read.format('parquet')
                 .load('s3a://bronze/ibge/estado'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA BONZE ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet:", df_estado_evd.count())
print('============!============!============!============!============!============!============!============!============')

df_estado_evd.show(5, False)
print('============!============!============!============!============!============!============!============!============')

df_estado_evd.printSchema()
print('============!============!============!============!============!============!============!============!============')
