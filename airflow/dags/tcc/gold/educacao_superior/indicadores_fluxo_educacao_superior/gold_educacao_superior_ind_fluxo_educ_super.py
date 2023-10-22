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
print('============!============!============!============!============!============!============!============!============!============')
print('============ Enriquecimento  da  base  de  Indicadores  com  Nome da Categoria Administrativa,  Nome da Organização  ============')
print('============ Acadêmica,   Nome do Tipo de Grau Acadêmico  e   Nome do Tipo de Modalidade de Ensino,   para Inclusão  ============')
print('============ na Base de Informaçõesde Indicadores de Fluxo da Educação Superior a ser utilizada na camada gold.      ============')
print('============!============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============!============')
print('============ Carregando  Dataframe  df_ind_fluxo_educ  com  informações  da  Base  de  Indicadores  da Camada SILVER ============')
print('============!============!============!============!============!============!============!============!============!============')

df_ind_fluxo_educ = spark.read.format('parquet').load('s3a://silver/educacao_superior/indicadores_fluxo_educacao_superior')
print('==========> df_ind_fluxo_educ.printSchema(): ', df_ind_fluxo_educ.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_ind_fluxo_educ.count(): ', df_ind_fluxo_educ.count())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_ind_fluxo_educ.show(5, False): ', df_ind_fluxo_educ.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Criação do Dataframe com as 4 novas colunas de enriquecimento de informações.              ============')
print('============!============!============!============!============!============!============!============!============')

df_ind_fluxo_educ_gold = df_ind_fluxo_educ.select(
        fn.col('codigo_instituicao'),
        fn.col('nome_instituicao'),
        fn.col('categoria_administrativa').alias('codigo_categoria_administrativa'),
        fn.when(fn.col('categoria_administrativa') == "1","Pública Federal")
          .when(fn.col('categoria_administrativa') == "2","Pública Estadual")
          .when(fn.col('categoria_administrativa') == "3","Pública Municipal")
          .when(fn.col('categoria_administrativa') == "4","Privada com Fins Lucrativos")
          .when(fn.col('categoria_administrativa') == "5","Privada sem Fins Lucrativos")
          .when(fn.col('categoria_administrativa') == "7","Especial")
          .otherwise('Outras')
          .alias('nome_categoria_administrativa'),
        fn.col('organizacao_academica').alias('codigo_organizacao_academica'),
        fn.when(fn.col('organizacao_academica') == "1","Universidade")
          .when(fn.col('organizacao_academica') == "2","Centro Universitário")
          .when(fn.col('organizacao_academica') == "3","Faculdade")
          .when(fn.col('organizacao_academica') == "4","Instituto Federal de Educação, Ciência e Tecnologia")
          .when(fn.col('organizacao_academica') == "5","Centro Federal de Educação Tecnológica")
          .otherwise('Outras')
          .alias('nome_organizacao_academica'),
        fn.col('codigo_curso_graduacao'),
        fn.col('nome_curso_graduacao'),
        fn.col('codigo_regiao_geografica_curso'),
        fn.col('nome_regiao_geografica_curso'),
        fn.col('codigo_unidade_federativa_curso'),
        fn.col('nome_unidade_federativa_curso'),
        fn.col('codigo_municipio_curso'),
        fn.col('nome_municipio_curso'),
        fn.col('tipo_grau_academico').alias('codigo_tipo_grau_academico'),
        fn.when(fn.col('tipo_grau_academico') == "1","Bacharelado")
          .when(fn.col('tipo_grau_academico') == "2","Licenciatura")
          .when(fn.col('tipo_grau_academico') == "3","Tecnológico")
          .otherwise('Outros')
          .alias('nome_tipo_grau_academico'),
        fn.col('tipo_modalidade_ensino').alias('codigo_tipo_modalidade_ensino'),
        fn.when(fn.col('tipo_modalidade_ensino') == "1","Presencial")
          .when(fn.col('tipo_modalidade_ensino') == "2","Curso a Distância")
          .otherwise('Outros')
          .alias('nome_tipo_modalidade_ensino'),
        fn.col('codigo_area_curso_classificacao_cine_brasil'),
        fn.col('nome_area_curso_classificacao_cine_brasil'),
        fn.col('codigo_grande_area_curso_classificacao_cine_brasil'),
        fn.col('nome_grande_area_curso_classificacao_cine_brasil'),
        fn.col('ano_ingresso'),
        fn.col('ano_referencia'),
        fn.col('prazo_integralizacao_anos'),
        fn.col('ano_integralizacao_curso'),
        fn.col('prazo_acompanhamento_curso_anos'),
        fn.col('ano_maximo_acompanhamento_curso'),
        fn.col('quantidade_ingressante_curso'),
        fn.col('quantidade_permanencia_curso_ano_referencia'),
        fn.col('quantidade_concluinte_curso_ano_referencia'),
        fn.col('quantidade_desistencia_curso_ano_referencia'),
        fn.col('quantidade_falecimento_curso_ano_referencia'),
        fn.col('taxa_permanencia_tap'),
        fn.col('taxa_conclusao_acumulada_tca'),
        fn.col('taxa_desistencia_acumulada_tda'),
        fn.col('taxa_conclusso_anual_tcan'),
        fn.col('taxa_desistencia_anual_tada')
)
print('==========> df_ind_fluxo_educ_gold.printSchema(): ', df_ind_fluxo_educ_gold.printSchema())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_ind_fluxo_educ_gold.count(): ', df_ind_fluxo_educ_gold.count())
print('============!============!============!============!============!============!============!============!============')
print('==========> df_ind_fluxo_educ_gold.show(5, False): ', df_ind_fluxo_educ_gold.show(5, False))
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando   Enriquecimento   de  Fluxo de Indicadores da Educação Superior  na  CAMADA GOLD ============')
print('============!============!============!============!============!============!============!============!============')

(df_ind_fluxo_educ_gold
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://gold/educacao_superior/indicadores_fluxo_educacao_superior')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA GOLD a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

df_fluxo_educ_superior_gold_evd = (spark.read.format('parquet')
                                        .load('s3a://gold/educacao_superior/indicadores_fluxo_educacao_superior'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA GOLD ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet - df_fluxo_educ_superior_gold_evd: ", df_fluxo_educ_superior_gold_evd.count())
print('============!============!============!============!============!============!============!============!============')
print('df_fluxo_educ_superior_gold_evd.show(5, False): ', df_fluxo_educ_superior_gold_evd.show(5, False))
print('============!============!============!============!============!============!============!============!============')
print('df_fluxo_educ_superior_gold_evd.printSchema(): ', df_fluxo_educ_superior_gold_evd.printSchema())
print('============!============!============!============!============!============!============!============!============')
