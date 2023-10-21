import os
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'FLAVIO TOKUO',
    'start_date': datetime(2023, 5, 1)
}

dag = DAG(dag_id='dag_tcc_silver_dado_geografico_municipio',
          default_args=default_args,
          schedule_interval='0 3 1 * *',
          tags=['TCC','SILVER']
      )

start_dag = DummyOperator(
                task_id='start_dag',
                dag=dag
                )

silver_dado_geografico_municipio = SparkSubmitOperator(
                          task_id='load_silver_dado_geografico_municipio',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar,\
                                /usr/local/airflow/jars/postgresql-42.3.3.jar'.replace(' ', ''),
                          application='/usr/local/airflow/dags/tcc/silver/dado_geografico/municipio/silver_dado_geografico_municipio.py',
                          dag=dag
                      )

dag_finish = DummyOperator(
                 task_id='dag_finish',
                 dag=dag
                 )

start_dag >> silver_dado_geografico_municipio >> dag_finish