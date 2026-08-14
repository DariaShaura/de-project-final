from logging import Logger
import vertica_python
from os.path import join,dirname, abspath
from pathlib import Path

class CdmGlobalMetricsLoader:

    _path_to_queries = join(dirname(abspath(__file__)),'queries')
    _file_name_gb_loader = 'realization_global_metrics_loader.sql'

    def __init__(self, v_conn_info, logger: Logger) -> None:
        self.v_conn_info = v_conn_info
        self.log = logger
        self.query_file = Path(join(self._path_to_queries,self._file_name_gb_loader))

    def run(self, date_update) -> bool:
        self.log.info(f'cdm for {date_update}')
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        try:
            if not self.query_file.is_file():
                self.log.error("Файл запроса не найден: %s", self.query_file)
                return False

            query = self.query_file.read_text(encoding="utf-8")
            if not query.strip():
                self.log.error("Файл запроса пуст: %s", self.query_file)
                return False
            
            with vertica_python.connect(**self.v_conn_info) as conn:
                with conn.cursor() as cur:
                    cur.execute(query,(str(date_update),))
                    conn.commit()
                    self.log.info("Запрос на добавление данных в global metrics успешно выполнен")
                    return True
        except Exception as e:
            self.log.error("Ошибка: %s", e, exc_info=True)
            return False
        
