MERGE INTO VT2605025A5430__DWH.global_metrics AS target
USING (
    WITH filtered_transactions AS (
        SELECT
            transaction_dttm::DATE AS date_update,
            currency_code,
            amount,
            operation_id,
            account_number_from
        FROM VT2605025A5430__STAGING.transactions
        WHERE status = 'done'
          AND transaction_type != 'authorisation'
          AND account_number_from > 0
          AND transaction_dttm::DATE = %s
    )
    SELECT
        ft.date_update,
        ft.currency_code AS currency_from,
        SUM(ft.amount*c.currency_code_div) AS amount_total, 
        COUNT(ft.operation_id) AS cnt_transactions,
        -- Приводим к NUMERIC, чтобы получить дробное среднее
        COUNT(ft.operation_id) * 1.0 / COUNT(DISTINCT ft.account_number_from) AS avg_transactions_per_account,
        COUNT(DISTINCT ft.account_number_from) AS cnt_accounts_make_transactions
    FROM filtered_transactions ft
    -- Присоединяем справочник валют, чтобы перевести в доллары
    INNER JOIN VT2605025A5430__STAGING.currencies c
        ON ft.date_update = c.date_update
        AND ft.currency_code = c.currency_code_with AND c.currency_code = '420'
    GROUP BY ft.date_update, ft.currency_code
) AS source
ON target.date_update = source.date_update
   AND target.currency_from = source.currency_from
WHEN MATCHED THEN UPDATE SET
    amount_total = source.amount_total,
    cnt_transactions = source.cnt_transactions,
    avg_transactions_per_account = source.avg_transactions_per_account,
    cnt_accounts_make_transactions = source.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN INSERT
    (date_update, currency_from, amount_total, cnt_transactions,
     avg_transactions_per_account, cnt_accounts_make_transactions)
    VALUES (
        source.date_update,
        source.currency_from,
        source.amount_total,
        source.cnt_transactions,
        source.avg_transactions_per_account,
        source.cnt_accounts_make_transactions
    );