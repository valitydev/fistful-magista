CREATE INDEX withdrawal_data_failure_index
    on mst.withdrawal_data (error_code, error_reason, error_sub_failure);