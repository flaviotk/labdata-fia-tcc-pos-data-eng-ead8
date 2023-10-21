import pyspark.sql.functions as fn
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
print('============ LEITURA das informações de Estados gravadas na Camada Bronze a partir da API do IBGE ============')
print('============!============!============!============!============!============!============!============!============')

df_estado = spark.read.format('parquet').load('s3a://bronze/ibge/estado')
print('==========> df_estado.printSchema(): ', df_estado.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_estado.show(5, False): ', df_estado.show(5, False))
print('============!============!============!============!============!============!============!============!============')


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Abertura da lista aninhada REGIAO em colunas para cada chave presente ============')
print('============!============!============!============!============!============!============!============!============')

df_estado_campos = df_estado.select(fn.col("id").cast('int').alias("estado_id"), 
                                    fn.col("nome").alias("estado_nome"), 
                                    fn.col("sigla").alias("estado_sigla"), 
                                    fn.col("regiao.id").cast('int').alias("regiao_id"),
                                    fn.col("regiao.nome").alias("regiao_nome"),
                                    fn.col("regiao.sigla").alias("regiao_sigla")
                                   )
print('==========> df_estado_campos.printSchema(): ', df_estado_campos.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_estado_campos.show(5, False): ', df_estado_campos.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando Dados Transformados de Estado na CAMADA SILVER ============')
print('============!============!============!============!============!============!============!============!============')

(df_estado_campos
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://silver/dado_geografico/estado')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA SILVER a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

df_estado_silver_evd = (spark.read.format('parquet')
                             .load('s3a://silver/dado_geografico/estado'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA SILVER ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet - df_estado_silver_evd: ", df_estado_silver_evd.count())
print('============!============!============!============!============!============!============!============!============')
print('df_estado_silver_evd.show(5, False): ', df_estado_silver_evd.show(5, False))
print('============!============!============!============!============!============!============!============!============')
print('df_estado_silver_evd.printSchema(): ', df_estado_silver_evd.printSchema())
print('============!============!============!============!============!============!============!============!============')
