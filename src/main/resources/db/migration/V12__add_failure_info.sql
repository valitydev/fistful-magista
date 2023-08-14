alter table mst.withdrawal_data
    add column error_code character varying;
alter table mst.withdrawal_data
    add column error_reason character varying;
alter table mst.withdrawal_data
    add column error_sub_failure character varying;
