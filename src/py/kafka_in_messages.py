from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
import time
from os.path import join,dirname, abspath
import psycopg2
from psycopg2.extras import execute_values


path_to_chck = join(dirname(abspath(__file__)))

TOPIC_NAME_IN = 'transaction-service-input' # Это топик, в который отпавляются данные о транзакциях и курсе валют
kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1a-lmdbpnqnkbr15lsr.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"producer_consumer\" password=\"producer_consumer\";',
}

# postrgesql with staging
postgresql_staging_settings = {
    "user": "db_user",
    "password": "student1987",
    "driver": "org.postgresql.Driver",
}

jdbc_staging_url = "jdbc:postgresql://rc1b-ckm819kf3m49vaqi.mdb.yandexcloud.net:6432/sprint9dwh"
# ?ssl=true&sslmode=verify-full

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
            # .config("spark.jars.repositories", "https://maven.aliyun.com/repository/public")

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
    
    # print('saving')
    # try:
    #     df.write.jdbc(
    #         url=jdbc_staging_url, 
    #         table="stg.transactions_currency", 
    #         mode="append", 
    #         properties=postgresql_staging_settings
    #     )
    #     print("PostgreSQL write successful")
        
    # except Exception as e:
    #     print(f'Error in writing to staging, {e}')
    # Собираем данные из DataFrame в список кортежей (для batch-вставки)
    # ВАЖНО: убедитесь, что порядок полей в списке соответствует столбцам таблицы
    rows = [tuple(row) for row in df.collect()]
    if not rows:
        return
    
    PG_HOST = "rc1b-ckm819kf3m49vaqi.mdb.yandexcloud.net"
    PG_PORT = 6432
    PG_DB = "sprint9dwh"
    PG_USER = postgresql_staging_settings['user']
    PG_PASSWORD = postgresql_staging_settings['password']

    # Подключаемся к PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    
    # Шаблон запроса — используем execute_values для высокой производительности
    insert_sql = """
        INSERT INTO stg.transactions_currency 
        (object_id, object_type, sent_dttm, payload) 
        VALUES %s
        ON CONFLICT (object_id) DO NOTHING
    """
    
    try:
        # execute_values автоматически формирует VALUES ( %s, %s, ... )
        execute_values(cur, insert_sql, rows)
        conn.commit()
        print(f"Successfully inserted {len(rows)} rows (conflicts ignored)")
    except Exception as e:
        conn.rollback()
        print(f"Error during insert: {e}")
        raise  # пробрасываем исключение, чтобы задача Airflow завершилась с ошибкой
    finally:
        cur.close()
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
    # message = message_from_restaurant(transactions)
    query = run_query(transactions)

    spark.stop()