from airflow.decorators import dag, task
import pendulum
import logging

# Теперь можно импортировать функцию
from stg_dag.currencies_to_stg import CurrenciesLoader
from stg_dag.transactions_to_stg import TransactionsLoader
from utils.db_utils import DBConfigFactory

log = logging.getLogger(__name__)

vertica_conn_info = DBConfigFactory.get_vertica_config('vertica_conn')
pg_conn_info = DBConfigFactory.get_postgres_config('pg_conn')
pg_conn1_info = DBConfigFactory.get_postgres_config('pg_conn1')

@dag(
    'stg_DAG',
    schedule_interval='0 7 * * *', 
    start_date=pendulum.datetime(2022, 10, 6, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    is_paused_upon_creation=False,  # Остановлен/запущен при появлении. Сразу запущен.
    max_active_runs=1
)
def stg():

    @task()
    def load_currencies(pg_conn_info, vertica_conn_info, ds=None):
        print(pg_conn_info)
        # Извлекаем параметры, переданные при запуске DAG
        target_date = ds
        # теперь используйте target_date в вашей логике
        loader = CurrenciesLoader(pg_conn_info, vertica_conn_info, log)
        loader.run(target_date)

    currencies_loader = load_currencies(pg_conn_info,vertica_conn_info)

    @task()
    def load_transactions(pg_conn_info, vertica_conn_info, ds=None):
        # Извлекаем параметры, переданные при запуске DAG
        target_date = ds
        # теперь используйте target_date в вашей логике
        loader = TransactionsLoader(pg_conn_info, vertica_conn_info, log)
        loader.run(target_date)

    transactions_loader = load_transactions(pg_conn1_info,vertica_conn_info)
    
    [currencies_loader,transactions_loader]
stg_dag = stg()  