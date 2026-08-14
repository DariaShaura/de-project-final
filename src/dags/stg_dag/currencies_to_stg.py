import json
import psycopg2
import vertica_python
from logging import Logger
import io

class CurrenciesLoader():

    BATCH_SIZE = 10000  # количество строк, читаемых из PostgreSQL за раз и отправляемых в Vertica

    def __init__(self, pg_conn_info, v_conn_info, logger: Logger) -> None:
        self.v_conn_info = v_conn_info
        self.pg_conn_info = pg_conn_info
        self.log = logger

    def run(self, date_update) -> bool:

        self.log.info(f'currencies for {date_update} to stg')
        # Подключаемся к PostgreSQL с серверным курсором
        try:
            with psycopg2.connect(**self.pg_conn_info) as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    pg_cursor.execute("""
                        SELECT object_id, payload, sent_dttm
                        FROM stg.transactions_currency
                        WHERE object_type = 'CURRENCY'
                        AND sent_dttm::date = %(target_date)s
                    """,  {'target_date': date_update})

                    rows = pg_cursor.fetchall()
        except Exception as e:
            self.log.error(f"Неизвестная ошибка при выполнении запроса: {e}")
            raise

        if not rows:
            self.log.info('no rows from postgresql')
            return

        self.log.info(f'{len(rows)} from postgresql')
        
        buffer = io.StringIO()

        for row in rows:
            # Обрабатываем каждую строку и заполняем буфер
            try:
                payload = json.loads(row[1])
                date_update = payload.get('date_update')
                currency_code = payload.get('currency_code')
                currency_code_with = payload.get('currency_code_with')
                currency_with_div = payload.get('currency_with_div')
                if not all([date_update is not None, currency_code is not None, currency_code_with is not None, currency_with_div is not None]):
                    self.log.info(f'не записано в буфер{payload}')
                    continue
                buffer.write(f"{date_update}|{currency_code}|{currency_code_with}|{currency_with_div}\n")
            except Exception as e:
                self.log.error(f"Skipping row {row[0]}: {e}")
                continue

        if buffer.tell() > 0:  # если есть данные
            with vertica_python.connect(**self.v_conn_info) as vertica_conn:
                with vertica_conn.cursor() as vertica_cursor:

                    try:
                        # Начинаем транзакцию
                        vertica_conn.autocommit = False  # если по умолчанию autocommit=True

                        # Удаляем записи за этот день
                        delete_sql = "DELETE FROM VT2605025A5430__STAGING.currencies WHERE date_update::date = %s"
                        vertica_cursor.execute(delete_sql, (date_update,))
                        self.log.info(f"Deleted existing records for {date_update}")

                        # Загружаем новые данные через COPY
                        buffer.seek(0)
                        vertica_cursor.copy("COPY VT2605025A5430__STAGING.currencies FROM STDIN DELIMITER '|'", buffer)
                        self.log.info(f"Inserted {len(rows)} new records")

                        # Фиксируем транзакцию
                        vertica_conn.commit()
                        self.log.info("Transaction committed successfully")

                    except Exception as e:
                        vertica_conn.rollback()
                        self.log.info(f"Error occurred, rolled back: {e}")
                        raise
        else:
            self.log.info('Buffer from postgresql is empty!')