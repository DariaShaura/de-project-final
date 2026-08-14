import logging
import pendulum
from airflow.decorators import dag, task

from cdm_dag.cdm_global_metrics_loader import CdmGlobalMetricsLoader
from utils.db_utils import DBConfigFactory


log = logging.getLogger(__name__)

vertica_conn_info = DBConfigFactory.get_vertica_config('vertica_conn')

@dag(
    'cdm_dag',
    schedule_interval='0 8 * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    is_paused_upon_creation=False,  # Остановлен/запущен при появлении. Сразу запущен.
    max_active_runs=1
)
def cdm():

    @task()
    def load_global_metrics(ds=None):

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CdmGlobalMetricsLoader(vertica_conn_info, log)

        target_date = ds
        # Запускаем построение витрины
        loader.run(target_date)

    global_metrics_loader = load_global_metrics()


    global_metrics_loader

cdm_dag = cdm()  
