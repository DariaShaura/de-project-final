CREATE LOCAL TEMP TABLE IF NOT EXISTS temp_transactions (
        operation_id varchar NOT NULL,
        account_number_from int NOT NULL,
        account_number_to int NOT NULL,
        currency_code char(3) NOT NULL,
        country varchar(80) NOT NULL,
        status varchar(80) NOT NULL,
        transaction_type varchar(80) NOT NULL,
        amount int NOT NULL,
        transaction_dt timestamp(3) NOT NULL
    ) ON COMMIT PRESERVE ROWS;