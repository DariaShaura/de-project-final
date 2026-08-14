from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from os.path import join,dirname, abspath
import psycopg2
from psycopg2.extras import execute_values
import json
import os
import sys


path_to_chck = join(dirname(abspath(__file__)))

TOPIC_NAME_IN = 'transaction-service-input' # Это топик, в который отпавляются данные о транзакциях и курсе валют

# parser = argparse.ArgumentParser()
# parser.add_argument('--all_config', required=True)
# args = parser.parse_args()

print("Python script started")
print("Arguments:", sys.argv)
print("Environment variables:", os.environ.keys())

config_str = os.environ.get('KAFKA_CONFIG_JSON')
if not config_str:
    raise RuntimeError("KAFKA_CONFIG_JSON not set")

conn = json.loads(config_str)
kafka_security_options = conn.get('kafka_yandex')

# postrgesql with staging
postgresql_staging_settings = conn.get('postgresql_yandex')

def spark_init(spark_name) -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0"
        ]
    )

    return (SparkSession.builder
            .master("local")
            .appName(spark_name)
            .config("spark.driver.extraJavaOptions", "-XX:-UseContainerSupport")
            .config("spark.executor.extraJavaOptions", "-XX:-UseContainerSupport")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
            )

def transform_transactions_kafka_stream(df: DataFrame) -> DataFrame:
    schema_value = StructType([
        StructField("object_id", StringType()),
        StructField("object_type", StringType()),
        StructField("sent_dttm", TimestampType()),
        StructField("payload", StringType()),
    ])
    
    return (df.withColumn('json_value', f.from_json(f.col('value').cast(StringType()),schema_value))\
    .where(f.col('json_value').isNotNull())
    .select(f.col('json_value.object_id').alias("object_id"),
            f.col('json_value.object_type').alias("object_type"),
            f.col('json_value.sent_dttm').alias("sent_dttm"),
            f.col('json_value.payload').alias("payload"))
    .where(f.col('object_id').isNotNull()))

def read_transactions_kafka_stream(spark):
    df_source= spark.readStream\
          .format('kafka')\
          .options(**kafka_security_options)\
          .option('subscribe',TOPIC_NAME_IN)\
          .option("startingOffsets", "earliest")\
          .load()

    return transform_transactions_kafka_stream(df_source)

# метод для записи данных сообщений в PostgreSQL
def foreach_batch_function(df, epoch_id):
    
    print(f"\nProcessing batch {epoch_id}")
    
    if df.count() == 0:
        print('batch is empty')
        return
    
    rows = [tuple(row) for row in df.collect()]
    if not rows:
        return

    # Подключаемся к PostgreSQL
    try:
        conn = psycopg2.connect(**postgresql_staging_settings)
        with conn:
            with conn.cursor() as cur:
                insert_sql = """
                    INSERT INTO stg.transactions_currency 
                    (object_id, object_type, sent_dttm, payload) 
                    VALUES %s
                    ON CONFLICT (object_id) DO NOTHING
                """
                execute_values(cur, insert_sql, rows)
                conn.commit()
                print(f"Successfully inserted {len(rows)} rows (conflicts ignored)")
    except Exception as e:
        conn.rollback()
        print(f"Error during insert: {e}")
        cur.close()
        conn.close()
        raise  # пробрасываем исключение, чтобы задача Airflow завершилась с ошибкой
    finally:
        if conn is not None:
            conn.close()

def run_query(df):
    return (df
           .writeStream \
            .foreachBatch(foreach_batch_function) \
            .option("checkpointLocation", join(path_to_chck,"checkpoint2"))\
            .trigger(once=True)\
            .start() \
            .awaitTermination()) 


if __name__ == "__main__":
    spark = spark_init('join stream')
    transactions = read_transactions_kafka_stream(spark)
    query = run_query(transactions)

    spark.stop()