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
print('============ LEITURA das informações de Municípios gravadas na Camada Bronze a partir da API do IBGE ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio = spark.read.format('parquet').load('s3a://bronze/ibge/municipio')
print('==========> df_municipio.printSchema(): ', df_municipio.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_municipio.show(5, False): ', df_municipio.show(5, False))
print('============!============!============!============!============!============!============!============!============')


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Abertura das listas e sublistas aninhadas de MICRORREGIAO e REGIAO_IMEDIATA em colunas para cada chave presente ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio_campos = (
   df_municipio.select(fn.col("id").alias("municipio_id"),
                       fn.col("nome").alias("municipio_nome"),
                       fn.col("microrregiao.id").alias('microregiao_id'),
                       fn.col("microrregiao.nome").alias('microregiao_nome'),
                       fn.col("microrregiao.mesorregiao.id").alias('microregiao_mesoregiao_id'),
                       fn.col("microrregiao.mesorregiao.nome").alias('microregiao_mesoregiao_nome'),
                       fn.col("microrregiao.mesorregiao.UF.id").alias('microregiao_mesoregiao_uf_id'),
                       fn.col("microrregiao.mesorregiao.UF.sigla").alias('microregiao_mesoregiao_uf_sigla'),
                       fn.col("microrregiao.mesorregiao.UF.nome").alias('microregiao_mesoregiao_uf_nome'),
                       fn.col("microrregiao.mesorregiao.UF.regiao.id").alias('microregiao_mesoregiao_uf_regiao_id'),
                       fn.col("microrregiao.mesorregiao.UF.regiao.sigla").alias('microregiao_mesoregiao_uf_regiao_sigla'),
                       fn.col("microrregiao.mesorregiao.UF.regiao.nome").alias('microregiao_mesoregiao_uf_regiao_nome'),
                       fn.col("regiao_imediata.id").alias('regiao_imediata_id'),
                       fn.col("regiao_imediata.nome").alias('regiao_imediata_nome'),
                       fn.col("regiao_imediata.regiao_intermediaria.id").alias('regiao_imediata_intermediaria_id'),
                       fn.col("regiao_imediata.regiao_intermediaria.nome").alias('regiao_imediata_intermediaria_nome'),
                       fn.col("regiao_imediata.regiao_intermediaria.UF.id").alias('regiao_imediata_intermediaria_uf_id'),
                       fn.col("regiao_imediata.regiao_intermediaria.UF.sigla").alias('regiao_imediata_intermediaria_uf_sigla'),
                       fn.col("regiao_imediata.regiao_intermediaria.UF.nome").alias('regiao_imediata_intermediaria_uf_nome'),
                       fn.col("regiao_imediata.regiao_intermediaria.UF.regiao.id").alias('regiao_imediata_intermediaria_uf_regiao_id'),
                       fn.col("regiao_imediata.regiao_intermediaria.UF.regiao.sigla").alias('regiao_imediata_intermediaria_uf_regiao_sigla'),
                       fn.col("regiao_imediata.regiao_intermediaria.UF.regiao.nome").alias('regiao_imediata_intermediaria_uf_regiao_nome')
                      )
                    )
print('==========> df_municipio_campos.printSchema(): ', df_municipio_campos.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_municipio_campos.show(5, False): ', df_municipio_campos.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando Dados Transformados de Município na CAMADA SILVER ============')
print('============!============!============!============!============!============!============!============!============')

(df_municipio_campos
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://silver/dado_geografico/municipio')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA SILVER a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio_silver_evd = (spark.read.format('parquet')
                             .load('s3a://silver/dado_geografico/municipio'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA SILVER ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet - df_municipio_silver_evd: ", df_municipio_silver_evd.count())
print('============!============!============!============!============!============!============!============!============')
print('df_municipio_silver_evd.show(5, False): ', df_municipio_silver_evd.show(5, False))
print('============!============!============!============!============!============!============!============!============')
print('df_municipio_silver_evd.printSchema(): ', df_municipio_silver_evd.printSchema())
print('============!============!============!============!============!============!============!============!============')
