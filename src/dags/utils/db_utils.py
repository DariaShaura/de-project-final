# db_utils.py
from airflow.hooks.base import BaseHook

class DBConfigFactory:
    @staticmethod
    def get_postgres_config(conn_id: str) -> dict:
        conn = BaseHook.get_connection(conn_id)
        config = {
            'host': conn.host,
            'port': conn.port,
            'user': conn.login,
            'password': conn.password,
            'database': conn.schema
        }
        return config

    @staticmethod
    def get_vertica_config(conn_id: str) -> dict:
        conn = BaseHook.get_connection(conn_id)
        config = {
            'host': conn.host,
            'port': conn.port,
            'user': conn.login,
            'password': conn.password,
            'autocommit': True,
        }
        return config
    @staticmethod
    def get_kafka_config(conn_id: str) -> dict:
        conn = BaseHook.get_connection(conn_id)
        return conn.extra_dejson