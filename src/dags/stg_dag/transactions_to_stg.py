from logging import Logger
import psycopg2
import vertica_python
import io
from os.path import join,dirname, abspath
from pathlib import Path


class TransactionsLoader():

    # Параметры батча
    BATCH_SIZE = 20000  # количество строк, читаемых из PostgreSQL за раз и отправляемых в Vertica
    _path_to_queries = join(dirname(abspath(__file__)),'queries')
    _file_name_tr_loader = 'realization_transactions_loader.sql'
    _file_name_cr_temp_tr = 'create_temp_transactions.sql'

    def __init__(self, pg_conn_info, v_conn_info, logger: Logger) -> None:
        self.v_conn_info = v_conn_info
        self.pg_conn_info = pg_conn_info
        self.log = logger
        self.query_merge_file = Path(join(self._path_to_queries,self._file_name_tr_loader))
        self.query_cr_temp_tr_file = Path(join(self._path_to_queries,self._file_name_cr_temp_tr))

    def run(self, date_update) -> bool:
        self.log.info(date_update)

        if not self.query_merge_file.is_file() or not self.query_cr_temp_tr_file.is_file():
            self.log.error("Файл запроса не найден: %s или %s", self.query_merge_file, self.query_cr_temp_tr_file)
            return False

        query_cr_temp = self.query_cr_temp_tr_file.read_text(encoding="utf-8")
        query_merge = self.query_merge_file.read_text(encoding="utf-8")
        if not query_merge.strip() or not query_cr_temp.strip():
            self.log.error("Файл запроса пуст: %s или %s", self.query_merge_file, self.query_cr_temp_tr_file)
            return False
        
        # Подключаемся к PostgreSQL с серверным курсором
        with vertica_python.connect(**self.v_conn_info) as vertica_conn:
            with vertica_conn.cursor() as vertica_cursor:
                with psycopg2.connect(**self.pg_conn_info) as pg_conn:
                
                    with pg_conn.cursor(name='my_cursor') as pg_cursor:
                        try:
                            pg_cursor.execute("""
                                SELECT operation_id, account_number_from, account_number_to, currency_code,
                                    country, status, transaction_type, amount, transaction_dt
                                FROM public.transactions
                                WHERE transaction_dt::date = %(target_date)s
                            """, {'target_date': date_update})
                        except Exception as e:
                            self.log.error(f"Error in postgresql public.transactions: {e}")
                            raise
                        # Подключаемся к Vertica и создаём временную таблицу
                        self.log.info('connecting to vertica')

                        try:
                            vertica_cursor.execute("DROP TABLE IF EXISTS temp_transactions")
                            vertica_cursor.execute(query_cr_temp)
                            vertica_conn.commit()
                            self.log.info('Temp table created successfully')
                        except Exception as e:
                            vertica_conn.rollback()
                            self.log.error(f"Error creating temp table: {e}")
                            raise  # прерываем выполнение задачи
                            
                        total_loaded = 0
                        buffer = io.StringIO()
                        rows_processed_in_batch = 0

                        while True:
                            # Читаем BATCH_SIZE строк из PostgreSQL
                            rows = pg_cursor.fetchmany(self.BATCH_SIZE)   
                            if not rows or (len(rows) == 0):
                                self.log.info('no rows from postgresql')
                                break
                            self.log.info(f'downloaded rows {len(rows)}')
                            # Обрабатываем каждую строку и заполняем буфер

                            for row in rows:
                                try:
                                    row_for_buffer = [str(value) if value is not None else None for value in row]
                                    if None in row_for_buffer:
                                        self.log.info(f'Skipping row {row[0]}')
                                        continue

                                    buffer.write('|'.join(row_for_buffer) + '\n')
                                    rows_processed_in_batch += 1
                                except Exception as e:
                                    self.log.error(f"Skipping row {row[0]}: {e}")
                                    continue

                            if buffer.tell() > 0:  # если есть данные
                                buffer.seek(0)
                                try:
                                    vertica_cursor.copy("COPY temp_transactions FROM STDIN DELIMITER '|'", buffer)
                                    vertica_conn.commit()
                
                                    total_loaded += rows_processed_in_batch
                                    self.log.info(f"Loaded batch of {rows_processed_in_batch} rows, total: {total_loaded}")
                                    # Очищаем буфер только после успешной загрузки
                                    buffer.truncate(0)
                                    buffer.seek(0)
                                    rows_processed_in_batch = 0
                                except Exception as e:
                                    buffer.truncate(0)
                                    buffer.seek(0)
                                    rows_processed_in_batch = 0
                                    vertica_conn.rollback()
                                    self.log.error(f"Error during COPY: {e}")
                                    raise
                # MERGE из temp_transactions в целевую таблицу
                try:
                    vertica_cursor.execute(query_merge)
                    vertica_conn.commit()
                    self.log.info(f"Successfully merged {total_loaded} currency records for {date_update}")
                except e:
                    self.log.error('Error in merge_sql',e)
                finally:
                    vertica_cursor.execute("DROP TABLE temp_transactions;")
                    vertica_conn.commit()