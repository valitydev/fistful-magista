ALTER TABLE mst.withdrawal_data
    ADD COLUMN IF NOT EXISTS changed_amount BIGINT,
    ADD COLUMN IF NOT EXISTS changed_currency_code CHARACTER VARYING;
