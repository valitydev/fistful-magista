CREATE INDEX withdrawal_provider_id_idx
    on mst.withdrawal_data (provider_id);
CREATE INDEX withdrawal_terminal_id_idx
    on mst.withdrawal_data (terminal_id);
