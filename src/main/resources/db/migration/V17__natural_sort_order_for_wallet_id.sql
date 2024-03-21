CREATE COLLATION numeric (provider = icu, locale = 'en@colNumeric=yes');
ALTER TABLE mst.wallet_data ALTER COLUMN wallet_id TYPE CHARACTER VARYING COLLATE numeric;
