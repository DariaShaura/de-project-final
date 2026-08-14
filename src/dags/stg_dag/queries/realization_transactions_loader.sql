MERGE INTO VT2605025A5430__STAGING.transactions AS target
    USING temp_transactions AS source
    ON target.operation_id = source.operation_id and target.transaction_dttm = source.transaction_dt 
    WHEN MATCHED THEN UPDATE SET
        account_number_from = source.account_number_from,
        account_number_to = source.account_number_to,
        currency_code = source.currency_code,
        country = source.country,
        status = source.status,
        transaction_type = source.transaction_type,
        amount = source.amount
    WHEN NOT MATCHED THEN INSERT
        (operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dttm)
        VALUES (source.operation_id,source.account_number_from,source.account_number_to,source.currency_code,source.country,source.status,source.transaction_type,source.amount,source.transaction_dt)