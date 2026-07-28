ALTER TABLE mst.withdrawal_data
    ADD COLUMN changed_amount BIGINT,
    ADD COLUMN changed_currency_code CHARACTER VARYING;
