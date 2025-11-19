-- ============================================
-- Consumed events table for idempotency
-- ============================================
create table if not exists consumed_events(
    event_id text primary key,
    received_at timestamptz not null default now()
);

create index if not exists idx_consumed_events_received_at on consumed_events(received_at);

-- ============================================
-- Dimension tables
-- ============================================
create table if not exists dim_users(
    user_id bigint primary key,
    username text,
    email text,
    role text not null,
    org text,
    region text,
    created_at timestamptz
);

create index if not exists idx_dim_users_role on dim_users(role);
create index if not exists idx_dim_users_region on dim_users(region) where region is not null;

create table if not exists dim_project (
    project_id text primary key,
    seller_id bigint not null,
    type text,
    region text,
    created_at timestamptz
);

create index if not exists idx_dim_project_seller on dim_project(seller_id);
create index if not exists idx_dim_project_region on dim_project(region) where region is not null;

-- ============================================
-- Fact tables with PARTITIONING
-- ============================================

-- fact_trade - Partitioned by executed_at (monthly)
create table if not exists fact_trade(
    order_id text not null,
    buyer_id bigint not null,
    seller_id bigint not null,
    listing_id text,
    quantity numeric(18,6) not null,
    unit text default 'tCO2e',
    unit_price numeric(18,6) not null,
    amount numeric(18,6) not null,
    currency text default 'USD',
    executed_at timestamptz not null,
    primary key (order_id, executed_at)
) partition by range (executed_at);


-- Create initial partitions (current month and next 3 months)
create table if not exists fact_trade_2024_11 partition of fact_trade
    for values from ('2024-11-01') to ('2024-12-01');

create table if not exists fact_trade_2024_12 partition of fact_trade
    for values from ('2024-12-01') to ('2025-01-01');

create table if not exists fact_trade_2025_01 partition of fact_trade
    for values from ('2025-01-01') to ('2025-02-01');

create table if not exists fact_trade_2025_02 partition of fact_trade
    for values from ('2025-02-01') to ('2025-03-01');

-- Indexes on partitioned table
create index if not exists idx_fact_trade_executed_at on fact_trade(executed_at);
create index if not exists idx_fact_trade_buyer on fact_trade(buyer_id, executed_at);
create index if not exists idx_fact_trade_seller on fact_trade(seller_id, executed_at);
create index if not exists idx_fact_trade_listing on fact_trade(listing_id, executed_at) where listing_id is not null;

-- fact_payment - Partitioned by completed_at (monthly)
create table if not exists fact_payment(
    payment_id text not null,
    order_id text not null,
    amount numeric(18,6) not null,
    currency text default 'USD',
    status text not null,
    payment_method text,
    completed_at timestamptz,
    primary key (payment_id, completed_at)
) partition by range (completed_at);

-- Create initial partitions
create table if not exists fact_payment_2024_11 partition of fact_payment
    for values from ('2024-11-01') to ('2024-12-01');

create table if not exists fact_payment_2024_12 partition of fact_payment
    for values from ('2024-12-01') to ('2025-01-01');

create table if not exists fact_payment_2025_01 partition of fact_payment
    for values from ('2025-01-01') to ('2025-02-01');

create table if not exists fact_payment_2025_02 partition of fact_payment
    for values from ('2025-02-01') to ('2025-03-01');

-- Indexes on partitioned table
create index if not exists idx_fact_payment_completed_at on fact_payment(completed_at);
create index if not exists idx_fact_payment_order on fact_payment(order_id, completed_at);
create index if not exists idx_fact_payment_status on fact_payment(status, completed_at);

-- fact_issuance - Partitioned by issued_at (monthly)
create table if not exists fact_issuance(
    issuance_id text not null,
    user_id bigint not null,
    request_id text,
    quantity_tco2e numeric(18,6) not null,
    distance_km numeric(12,2),
    energy_kwh numeric(12,2),
    co2_avoided_kg numeric(12,2),
    issued_at timestamptz not null,
    primary key (issuance_id, issued_at)
) partition by range (issued_at);

-- Create initial partitions
create table if not exists fact_issuance_2024_11 partition of fact_issuance
    for values from ('2024-11-01') to ('2024-12-01');

create table if not exists fact_issuance_2024_12 partition of fact_issuance
    for values from ('2024-12-01') to ('2025-01-01');

create table if not exists fact_issuance_2025_01 partition of fact_issuance
    for values from ('2025-01-01') to ('2025-02-01');

create table if not exists fact_issuance_2025_02 partition of fact_issuance
    for values from ('2025-02-01') to ('2025-03-01');

-- Indexes on partitioned table
create index if not exists idx_fact_issuance_issued_at on fact_issuance(issued_at);
create index if not exists idx_fact_issuance_user on fact_issuance(user_id, issued_at);
create index if not exists idx_fact_issuance_request on fact_issuance(request_id, issued_at) where request_id is not null;

-- fact_user_activity - Partitioned by occurred_at (monthly)
create table if not exists fact_user_activity(
    id bigserial,
    user_id bigint not null,
    event_type text not null,
    event_data jsonb,
    occurred_at timestamptz not null,
    primary key (id, occurred_at)
) partition by range (occurred_at);

-- Create initial partitions
create table if not exists fact_user_activity_2024_11 partition of fact_user_activity
    for values from ('2024-11-01') to ('2024-12-01');

create table if not exists fact_user_activity_2024_12 partition of fact_user_activity
    for values from ('2024-12-01') to ('2025-01-01');

create table if not exists fact_user_activity_2025_01 partition of fact_user_activity
    for values from ('2025-01-01') to ('2025-02-01');

create table if not exists fact_user_activity_2025_02 partition of fact_user_activity
    for values from ('2025-02-01') to ('2025-03-01');

-- Indexes on partitioned table
create index if not exists idx_fact_user_activity_user on fact_user_activity(user_id, occurred_at);
create index if not exists idx_fact_user_activity_occurred on fact_user_activity(occurred_at);
create index if not exists idx_fact_user_activity_type on fact_user_activity(event_type, occurred_at);
create index if not exists idx_fact_user_activity_event_data on fact_user_activity using gin(event_data);

-- ============================================
-- Partition management metadata table
-- ============================================
create table if not exists partition_metadata(
    table_name text primary key,
    last_partition_date date not null,
    partition_interval text not null default 'monthly',
    retention_months int default 12,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

-- Insert metadata for partitioned tables
insert into partition_metadata (table_name, last_partition_date, partition_interval, retention_months)
values
    ('fact_trade', '2025-02-01', 'monthly', 24),
    ('fact_payment', '2025-02-01', 'monthly', 24),
    ('fact_issuance', '2025-02-01', 'monthly', 24),
    ('fact_user_activity', '2025-02-01', 'monthly', 12)
on conflict (table_name) do nothing;

-- ============================================
-- Materialized views for reporting
-- ============================================

-- MV for daily trades
create materialized view if not exists mv_trades_daily as
select date_trunc('day', executed_at) as day,
       sum(quantity) as credits_sold,
       sum(amount) as revenue,
       count(*) as trade_count,
       avg(unit_price) as avg_unit_price,
       count(distinct buyer_id) as unique_buyers,
       count(distinct seller_id) as unique_sellers
from fact_trade
group by 1;

create unique index if not exists idx_mv_trades_daily_day on mv_trades_daily(day);

-- MV for daily issuances
create materialized view if not exists mv_issuance_daily as
select date_trunc('day', issued_at) as day,
       sum(quantity_tco2e) as credits_issued,
       count(*) as issuance_count,
       avg(quantity_tco2e) as avg_issuance,
       count(distinct user_id) as unique_users
from fact_issuance
group by 1;

create unique index if not exists idx_mv_issuance_daily_day on mv_issuance_daily(day);

-- MV for daily payments
create materialized view if not exists mv_payments_daily as
select date_trunc('day', completed_at) as day,
       sum(amount) as total_payments,
       count(*) as payment_count,
       avg(amount) as avg_payment,
       count(distinct payment_method) as payment_methods_used
from fact_payment
where status = 'COMPLETED'
group by 1;

create unique index if not exists idx_mv_payments_daily_day on mv_payments_daily(day);

-- MV for user activity summary
create materialized view if not exists mv_user_activity_daily as
select date_trunc('day', occurred_at) as day,
       event_type,
       count(*) as event_count,
       count(distinct user_id) as unique_users
from fact_user_activity
group by 1, 2;

create unique index if not exists idx_mv_user_activity_daily_day_type on mv_user_activity_daily(day, event_type);

-- MV refresh metadata table
create table if not exists mv_refresh_log(
    id bigserial primary key,
    mv_name text not null,
    refresh_started_at timestamptz not null,
    refresh_completed_at timestamptz,
    status text not null, -- RUNNING, SUCCESS, FAILED
    error_message text,
    rows_affected bigint
);

create index if not exists idx_mv_refresh_log_mv_name on mv_refresh_log(mv_name, refresh_started_at desc);

-- ============================================
-- Helper function to create next partition
-- ============================================
create or replace function create_next_partition(
    p_table_name text,
    p_start_date date,
    p_end_date date
) returns text as $$
declare
    v_partition_name text;
    v_sql text;
begin
    v_partition_name := p_table_name || '_' || to_char(p_start_date, 'YYYY_MM');

    v_sql := format(
        'create table if not exists %I partition of %I for values from (%L) to (%L)',
        v_partition_name,
        p_table_name,
        p_start_date,
        p_end_date
    );

    execute v_sql;

    return v_partition_name;
end;
$$ language plpgsql;

-- ============================================
-- Comments for documentation
-- ============================================
comment on table fact_trade is 'Partitioned fact table for trade transactions';
comment on table fact_payment is 'Partitioned fact table for payment transactions';
comment on table fact_issuance is 'Partitioned fact table for credit issuances';
comment on table fact_user_activity is 'Partitioned fact table for user activities';
comment on table partition_metadata is 'Metadata for managing table partitions';
comment on table mv_refresh_log is 'Log of materialized view refresh operations';
