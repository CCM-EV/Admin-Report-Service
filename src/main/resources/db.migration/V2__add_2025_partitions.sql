-- V2__add_2025_partitions.sql
create table if not exists fact_trade_2025_03 partition of fact_trade
    for values from ('2025-03-01') to ('2025-04-01');

create table if not exists fact_trade_2025_04 partition of fact_trade
    for values from ('2025-04-01') to ('2025-05-01');

create table if not exists fact_trade_2025_05 partition of fact_trade
    for values from ('2025-05-01') to ('2025-06-01');

create table if not exists fact_trade_2025_06 partition of fact_trade
    for values from ('2025-06-01') to ('2025-07-01');

create table if not exists fact_trade_2025_07 partition of fact_trade
    for values from ('2025-07-01') to ('2025-08-01');

create table if not exists fact_trade_2025_08 partition of fact_trade
    for values from ('2025-08-01') to ('2025-09-01');

create table if not exists fact_trade_2025_09 partition of fact_trade
    for values from ('2025-09-01') to ('2025-10-01');

create table if not exists fact_trade_2025_10 partition of fact_trade
    for values from ('2025-10-01') to ('2025-11-01');

create table if not exists fact_trade_2025_11 partition of fact_trade
    for values from ('2025-11-01') to ('2025-12-01');

create table if not exists fact_trade_2025_12 partition of fact_trade
    for values from ('2025-12-01') to ('2026-01-01');

-- fact_payment partitions for 2025 (March to December)
create table if not exists fact_payment_2025_03 partition of fact_payment
    for values from ('2025-03-01') to ('2025-04-01');

create table if not exists fact_payment_2025_04 partition of fact_payment
    for values from ('2025-04-01') to ('2025-05-01');

create table if not exists fact_payment_2025_05 partition of fact_payment
    for values from ('2025-05-01') to ('2025-06-01');

create table if not exists fact_payment_2025_06 partition of fact_payment
    for values from ('2025-06-01') to ('2025-07-01');

create table if not exists fact_payment_2025_07 partition of fact_payment
    for values from ('2025-07-01') to ('2025-08-01');

create table if not exists fact_payment_2025_08 partition of fact_payment
    for values from ('2025-08-01') to ('2025-09-01');

create table if not exists fact_payment_2025_09 partition of fact_payment
    for values from ('2025-09-01') to ('2025-10-01');

create table if not exists fact_payment_2025_10 partition of fact_payment
    for values from ('2025-10-01') to ('2025-11-01');

create table if not exists fact_payment_2025_11 partition of fact_payment
    for values from ('2025-11-01') to ('2025-12-01');

create table if not exists fact_payment_2025_12 partition of fact_payment
    for values from ('2025-12-01') to ('2026-01-01');

-- fact_issuance partitions for 2025 (March to December)
create table if not exists fact_issuance_2025_03 partition of fact_issuance
    for values from ('2025-03-01') to ('2025-04-01');

create table if not exists fact_issuance_2025_04 partition of fact_issuance
    for values from ('2025-04-01') to ('2025-05-01');

create table if not exists fact_issuance_2025_05 partition of fact_issuance
    for values from ('2025-05-01') to ('2025-06-01');

create table if not exists fact_issuance_2025_06 partition of fact_issuance
    for values from ('2025-06-01') to ('2025-07-01');

create table if not exists fact_issuance_2025_07 partition of fact_issuance
    for values from ('2025-07-01') to ('2025-08-01');

create table if not exists fact_issuance_2025_08 partition of fact_issuance
    for values from ('2025-08-01') to ('2025-09-01');

create table if not exists fact_issuance_2025_09 partition of fact_issuance
    for values from ('2025-09-01') to ('2025-10-01');

create table if not exists fact_issuance_2025_10 partition of fact_issuance
    for values from ('2025-10-01') to ('2025-11-01');

create table if not exists fact_issuance_2025_11 partition of fact_issuance
    for values from ('2025-11-01') to ('2025-12-01');

create table if not exists fact_issuance_2025_12 partition of fact_issuance
    for values from ('2025-12-01') to ('2026-01-01');

-- fact_user_activity partitions for 2025 (March to December)
create table if not exists fact_user_activity_2025_03 partition of fact_user_activity
    for values from ('2025-03-01') to ('2025-04-01');

create table if not exists fact_user_activity_2025_04 partition of fact_user_activity
    for values from ('2025-04-01') to ('2025-05-01');

create table if not exists fact_user_activity_2025_05 partition of fact_user_activity
    for values from ('2025-05-01') to ('2025-06-01');

create table if not exists fact_user_activity_2025_06 partition of fact_user_activity
    for values from ('2025-06-01') to ('2025-07-01');

create table if not exists fact_user_activity_2025_07 partition of fact_user_activity
    for values from ('2025-07-01') to ('2025-08-01');

create table if not exists fact_user_activity_2025_08 partition of fact_user_activity
    for values from ('2025-08-01') to ('2025-09-01');

create table if not exists fact_user_activity_2025_09 partition of fact_user_activity
    for values from ('2025-09-01') to ('2025-10-01');

create table if not exists fact_user_activity_2025_10 partition of fact_user_activity
    for values from ('2025-10-01') to ('2025-11-01');

create table if not exists fact_user_activity_2025_11 partition of fact_user_activity
    for values from ('2025-11-01') to ('2025-12-01');

create table if not exists fact_user_activity_2025_12 partition of fact_user_activity
    for values from ('2025-12-01') to ('2026-01-01');

-- Update partition metadata
update partition_metadata 
set last_partition_date = '2025-12-01',
    updated_at = now()
where table_name in ('fact_trade', 'fact_payment', 'fact_issuance', 'fact_user_activity');

-- Add 2026 Q1 partitions as well for continuity
create table if not exists fact_trade_2026_01 partition of fact_trade
    for values from ('2026-01-01') to ('2026-02-01');

create table if not exists fact_payment_2026_01 partition of fact_payment
    for values from ('2026-01-01') to ('2026-02-01');

create table if not exists fact_issuance_2026_01 partition of fact_issuance
    for values from ('2026-01-01') to ('2026-02-01');

create table if not exists fact_user_activity_2026_01 partition of fact_user_activity
    for values from ('2026-01-01') to ('2026-02-01');
