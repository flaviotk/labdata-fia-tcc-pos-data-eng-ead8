import pandas as pd
import glob
import zipfile
import requests
import os
from os import chdir, getcwd, listdir
from io import BytesIO

import pyspark.sql.functions as fn
from pyspark.sql.types import *
from pyspark.sql import SparkSession
#-------------------------------
# bibliotecas webscraping
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings()

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
print('============ Carregando Variáveis de Ambiente ============')
print('============!============!============!============!============!============!============!============!============')

url = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/indicadores-de-fluxo-da-educacao-superior/2010-2019'
folder = 'indicadores_fluxo_educacao_superior'
path = '/tmp/'

print('==========> url webscraping:', url)
print('==========> folder:', folder)
print('==========> path:', path)
print('============!============!============!============!============!============!============!============!============')


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Criando diretório para armazenar o conteúdo do INEP ============')
print('============!============!============!============!============!============!============!============!============')
os.makedirs(folder, exist_ok=True)

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Efetuando busca webscraping da URL de download do arquivo ZIP ============')
print('============!============!============!============!============!============!============!============!============')
conexao = urllib3.PoolManager()
retorno = conexao.request('GET', url)

pagina = BeautifulSoup(retorno.data,"html.parser")

dado = []
for link in pagina.find_all('a',class_ = 'external-link'):
    dado.append(link.get('href'))

url_download = str(dado[0])

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Efetuando download do arquivo Zip ============')
print('============!============!============!============!============!============!============!============!============')
# Desabilitar temporariamente a verificação SSL
response = requests.get(url_download, verify=False)
filebytes = BytesIO(response.content)

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Descompactando arquivo Zip no Lake ============')
print('============!============!============!============!============!============!============!============!============')

myzip = zipfile.ZipFile(filebytes)
myzip.extractall(path + folder)

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Descompactação efetuada ============')
print('============!============!============!============!============!============!============!============!============')

chdir(path + folder)
print(getcwd())
for c in listdir():
    print(c)
print('============!============!============!============!============!============!============!============!============')

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Efetuando Busca do nome do Arquivo XLSX ============')
print('============!============!============!============!============!============!============!============!============')

targetPattern = r"{path}{folder}/*.xlsx".format(path=path, folder=folder)
file = str(glob.glob(targetPattern)).replace("['","").replace("']","")
print("=======> Arquivo:", file)
print('============!============!============!============!============!============!============!============!============')

print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe com os dados do Excel ============')
print('============!============!============!============!============!============!============!============!============')

df = pd.read_excel(file, usecols= 'A:AE', skiprows= lambda x: x < 8 or x > 259238, sheet_name='INDICADORES_TRAJETORIA' )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Convertendo Dataframe para SPARK ============')
print('============!============!============!============!============!============!============!============!============')

sparkDF=spark.createDataFrame(df) 

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Ajuste de Tipo de Dado dos Campos do Dataframe antes da Gravação no Data Lake ============')
print('============!============!============!============!============!============!============!============!============')

sparkDF.printSchema()
print('============!============!============!============!============!============!============!============!============')

DFAjusteTipoDado = (sparkDF
    .select(fn.col('CO_IES').cast('bigint').alias('CO_IES'),
            fn.col('NO_IES').cast('string').alias('NO_IES'),
            fn.col('TP_CATEGORIA_ADMINISTRATIVA').cast('int').alias('TP_CATEGORIA_ADMINISTRATIVA'),
            fn.col('TP_ORGANIZACAO_ACADEMICA').cast('int').alias('TP_ORGANIZACAO_ACADEMICA'),
            fn.col('CO_CURSO').cast('bigint').alias('CO_CURSO'),
            fn.col('NO_CURSO').cast('string').alias('NO_CURSO'),
            fn.when(fn.trim(fn.col('CO_REGIAO')) == 'NaN', None).otherwise(fn.col('CO_REGIAO')).cast('int').alias('CO_REGIAO'),
            fn.when(fn.trim(fn.col('CO_UF')) == 'NaN', None).otherwise(fn.col('CO_UF')).cast('int').alias('CO_UF'),
            fn.when(fn.trim(fn.col('CO_MUNICIPIO')) == 'NaN', None).otherwise(fn.col('CO_MUNICIPIO')).cast('bigint').alias('CO_MUNICIPIO'),
            fn.col('TP_GRAU_ACADEMICO').cast('int').alias('TP_GRAU_ACADEMICO'),
            fn.col('TP_MODALIDADE_ENSINO').cast('int').alias('TP_MODALIDADE_ENSINO'),
            fn.col('CO_CINE_ROTULO').cast('string').alias('CO_CINE_ROTULO'),
            fn.col('NO_CINE_ROTULO').cast('string').alias('NO_CINE_ROTULO'),
            fn.col('CO_CINE_AREA_GERAL').cast('int').alias('CO_CINE_AREA_GERAL'),
            fn.col('NO_CINE_AREA_GERAL').cast('string').alias('NO_CINE_AREA_GERAL'),
            fn.col('NU_ANO_INGRESSO').cast('int').alias('NU_ANO_INGRESSO'),
            fn.col('NU_ANO_REFERENCIA').cast('int').alias('NU_ANO_REFERENCIA'),
            fn.col('NU_PRAZO_INTEGRALIZACAO').cast('int').alias('NU_PRAZO_INTEGRALIZACAO'),
            fn.col('NU_ANO_INTEGRALIZACAO').cast('int').alias('NU_ANO_INTEGRALIZACAO'),
            fn.col('NU_PRAZO_ACOMPANHAMENTO').cast('int').alias('NU_PRAZO_ACOMPANHAMENTO'),
            fn.col('NU_ANO_MAXIMO_ACOMPANHAMENTO').cast('int').alias('NU_ANO_MAXIMO_ACOMPANHAMENTO'),
            fn.col('QT_INGRESSANTE').cast('bigint').alias('QT_INGRESSANTE'),
            fn.col('QT_PERMANENCIA').cast('bigint').alias('QT_PERMANENCIA'),
            fn.col('QT_CONCLUINTE').cast('bigint').alias('QT_CONCLUINTE'),
            fn.col('QT_DESISTENCIA').cast('bigint').alias('QT_DESISTENCIA'),
            fn.col('QT_FALECIDO').cast('bigint').alias('QT_FALECIDO'),
            fn.col('TAP').cast('double').alias('TAP'),
            fn.col('TCA').cast('double').alias('TCA'),
            fn.col('TDA').cast('double').alias('TDA'),
            fn.col('TCAN').cast('double').alias('TCAN'),
            fn.col('TADA').cast('double').alias('TADA')
           )
)
DFAjusteTipoDado.printSchema()
print('============!============!============!============!============!============!============!============!============')


print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Gravando Dados na CAMADA BRONZE  ============')
print('============!============!============!============!============!============!============!============!============')

(DFAjusteTipoDado
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://bronze/inep/educacao_superior/indicadores_fluxo_educacao_superior')
 )

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Carregando Dataframe da CAMADA BRONZE a partir do Arquivo Parquet ============')
print('============!============!============!============!============!============!============!============!============')

dfIndicador = (spark.read.format('parquet')
             .load('s3a://bronze/inep/educacao_superior/indicadores_fluxo_educacao_superior'))

print('   ')
print('============!============!============!============!============!============!============!============!============')
print('============ Evidência de Gravação na CAMADA BONZE ============')
print('============!============!============!============!============!============!============!============!============')

print("Total de registros Carregados no Arquivo Parquet:", dfIndicador.count())
print('============!============!============!============!============!============!============!============!============')

dfIndicador.show(5, False)
print('============!============!============!============!============!============!============!============!============')

dfIndicador.printSchema()
print('============!============!============!============!============!============!============!============!============')
