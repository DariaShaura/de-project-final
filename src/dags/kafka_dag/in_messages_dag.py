from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import logging
import json

from utils.db_utils import DBConfigFactory

os.environ['JAVA_HOME']='/usr/local/openjdk-11'
DAG_DIR = os.path.dirname(os.path.abspath(__file__))

log = logging.getLogger(__name__)

@dag(
    dag_id="kafka_message_dag",
    default_args={'owner': 'airflow', 'start_date': datetime(2022, 5, 25)},
    schedule_interval='0 6 * * *',
    catchup=False,
    is_paused_upon_creation=False,  # Остановлен/запущен при появлении. Сразу запущен.
    max_active_runs=1
)
def pre_stg_dag():

    @task
    def prepare_config() -> str:
        kafka_config = DBConfigFactory.get_kafka_config('kafka_yandex')
        pg_conn_info = DBConfigFactory.get_postgres_config('pg_conn')
        all_config = {
            'kafka_yandex': kafka_config,
            'postgresql_yandex': pg_conn_info
        }
        return json.dumps(all_config)

    config_json = prepare_config()

    spark_task = SparkSubmitOperator(
        task_id='get_input_kafka_data',
        application=os.path.join(DAG_DIR, 'kafka_in_messages.py'),
        # application_args=['--all_config', config_json],  
        spark_binary='/usr/local/bin/spark-submit',
        env_vars={
        'KAFKA_CONFIG_JSON': config_json  # весь конфиг в одну переменную
        },
        conf={
            "spark.master": "local[*]",
            "spark.driver.maxResultSize": "20g",
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3,org.postgresql:postgresql:42.4.0",
            "spark.driver.extraJavaOptions": "-Djdk.lang.Process.launchMechanism=vfork -XX:-UseContainerSupport",
            "spark.executor.extraJavaOptions": "-Djdk.lang.Process.launchMechanism=vfork -XX:-UseContainerSupport"
        },
        executor_cores=1,
        executor_memory='2g',
        driver_memory='2g',
        verbose=True
    )

    # Явная зависимость: spark_task выполняется после prepare_config
    config_json >> spark_task

dag = pre_stg_dag()
