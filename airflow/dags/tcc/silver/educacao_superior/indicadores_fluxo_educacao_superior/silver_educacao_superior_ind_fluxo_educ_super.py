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
print('============ Recuperação das informações de Nome de Estado, Nome de Município e Nome de Região ============')
print('============ para Inclusão na Base de Informações de Indicadores de Fluxo da Educação Superior ============')
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe df_estado com informações da Base de Estado da Camada SILVER ============')
print('============!============!============!============!============!============!============!============!============')

df_estado = spark.read.format('parquet').load('s3a://silver/dado_geografico/estado')
print('==========> df_estado.printSchema(): ', df_estado.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_estado.count(): ', df_estado.count())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_estado.show(5, False): ', df_estado.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe df_municipio com informações da Base de Município da Camada SILVER ============')
print('============!============!============!============!============!============!============!============!============')

df_municipio = spark.read.format('parquet').load('s3a://silver/dado_geografico/municipio')
print('==========> df_municipio.printSchema(): ', df_municipio.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_municipio.count(): ', df_municipio.count())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_municipio.show(5, False): ', df_municipio.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe df_fluxo_educ_superior com as informações de Indicadores de Fluxo da Educação Superior ============')
print('============ que foram recuperados no site do INEP através de webscraping e gravados na Camada BRONZE   ============')
print('============!============!============!============!============!============!============!============!============')

df_fluxo_educ_superior = spark.read.format('parquet').load('s3a://bronze/inep/educacao_superior/indicadores_fluxo_educacao_superior')
print('==========> df_fluxo_educ_superior.printSchema(): ', df_fluxo_educ_superior.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_fluxo_educ_superior.count(): ', df_fluxo_educ_superior.count())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_fluxo_educ_superior.show(5, False): ', df_fluxo_educ_superior.show(5, False))
print('============!============!============!============!============!============!============!============!============')


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Incorporando  as  informações  de  Nome de Estado,   Nome de Município  e  Nome de Região   no ============')
print('============ dataframe df_fluxo_educ_superior_estmun através de join com as bases df_estado e df_municipio. ============')
print('============ Mantidos  os dados do  df_fluxo_educ_superior,  através  de  left  join  com  as  duas  bases. ============')
print('============!============!============!============!============!============!============!============!============')

df_fluxo_educ_superior_estmun = (df_fluxo_educ_superior.alias('educ')
     .join(df_estado.alias('est'), fn.col('educ.CO_UF') == fn.col('est.estado_id'), 'left')
     .join(df_municipio.alias('mun'), fn.col('educ.CO_MUNICIPIO') == fn.col('mun.municipio_id'), 'left')
     .select(fn.col('educ.CO_IES').alias('codigo_instituicao'),
             fn.col('educ.NO_IES').alias('nome_instituicao'),
             fn.col('educ.TP_CATEGORIA_ADMINISTRATIVA').alias('categoria_administrativa'),
             fn.col('educ.TP_ORGANIZACAO_ACADEMICA').alias('organizacao_academica'),
             fn.col('educ.CO_CURSO').alias('codigo_curso_graduacao'),
             fn.col('educ.NO_CURSO').alias('nome_curso_graduacao'),
             fn.col('educ.CO_REGIAO').alias('codigo_regiao_geografica_curso'),
             fn.col('est.regiao_nome').alias('nome_regiao_geografica_curso'), # df_estado.regiao_nome
             fn.col('educ.CO_UF').alias('codigo_unidade_federativa_curso'),
             fn.col('est.estado_nome').alias('nome_unidade_federativa_curso'), # df_estado.estado_nome
             fn.col('educ.CO_MUNICIPIO').alias('codigo_municipio_curso'),
             fn.col('mun.municipio_nome').alias('nome_municipio_curso'), # df_municipio.municipio_nome
             fn.col('educ.TP_GRAU_ACADEMICO').alias('tipo_grau_academico'),
             fn.col('educ.TP_MODALIDADE_ENSINO').alias('tipo_modalidade_ensino'),
             fn.col('educ.CO_CINE_ROTULO').alias('codigo_area_curso_classificacao_cine_brasil'),
             fn.col('educ.NO_CINE_ROTULO').alias('nome_area_curso_classificacao_cine_brasil'),
             fn.col('educ.CO_CINE_AREA_GERAL').alias('codigo_grande_area_curso_classificacao_cine_brasil'),
             fn.col('educ.NO_CINE_AREA_GERAL').alias('nome_grande_area_curso_classificacao_cine_brasil'),
             fn.col('educ.NU_ANO_INGRESSO').alias('ano_ingresso'),
             fn.col('educ.NU_ANO_REFERENCIA').alias('ano_referencia'),
             fn.col('educ.NU_PRAZO_INTEGRALIZACAO').alias('prazo_integralizacao_anos'),
             fn.col('educ.NU_ANO_INTEGRALIZACAO').alias('ano_integralizacao_curso'),
             fn.col('educ.NU_PRAZO_ACOMPANHAMENTO').alias('prazo_acompanhamento_curso_anos'),
             fn.col('educ.NU_ANO_MAXIMO_ACOMPANHAMENTO').alias('ano_maximo_acompanhamento_curso'),
             fn.col('educ.QT_INGRESSANTE').alias('quantidade_ingressante_curso'),
             fn.col('educ.QT_PERMANENCIA').alias('quantidade_permanência_curso_ano_referencia'),
             fn.col('educ.QT_CONCLUINTE').alias('quantidade_concluinte_curso_ano_referencia'),
             fn.col('educ.QT_DESISTENCIA').alias('quantidade_desistencia_curso_ano_referencia'),
             fn.col('educ.QT_FALECIDO').alias('quantidade_falecimento_curso_ano_referencia'),
             fn.col('educ.TAP').alias('taxa_permanencia_tap'),
             fn.col('educ.TCA').alias('taxa_conclusao_acumulada_tca'), 
             fn.col('educ.TDA').alias('taxa_desistencia_acumulada_tda'),
             fn.col('educ.TCAN').alias('taxa_conclusso_anual_tcan'),
             fn.col('educ.TADA').alias('taxa_desistencia_anual_tada')
            )
)
print('==========> df_fluxo_educ_superior_estmun.printSchema(): ', df_fluxo_educ_superior_estmun.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_fluxo_educ_superior_estmun.count(): ', df_fluxo_educ_superior_estmun.count())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_fluxo_educ_superior_estmun.show(5, False): ', df_fluxo_educ_superior_estmun.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando Dados Transformados de Fluxo de Indicadores da Educação Superior na CAMADA SILVER ============')
print('============!============!============!============!============!============!============!============!============')

(df_fluxo_educ_superior_estmun
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://silver/educacao_superior/indicadores_fluxo_educacao_superior')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA SILVER a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

df_fluxo_educ_superior_silver_evd = (spark.read.format('parquet')
                             .load('s3a://silver/educacao_superior/indicadores_fluxo_educacao_superior'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA SILVER ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet - df_fluxo_educ_superior_silver_evd: ", df_fluxo_educ_superior_silver_evd.count())
print('============!============!============!============!============!============!============!============!============')
print('df_fluxo_educ_superior_silver_evd.show(5, False): ', df_fluxo_educ_superior_silver_evd.show(5, False))
print('============!============!============!============!============!============!============!============!============')
print('df_fluxo_educ_superior_silver_evd.printSchema(): ', df_fluxo_educ_superior_silver_evd.printSchema())
print('============!============!============!============!============!============!============!============!============')
