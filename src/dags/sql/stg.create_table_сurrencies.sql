drop table if exists STV2024031233__STAGING.сurrencies;
create table if not exists STV2024031233__STAGING.сurrencies(
date_update timestamp,
currency_code int,
currency_code_with int,
currency_with_div numeric(18, 2),
constraint pk_сurrencies primary key(date_update, currency_code, currency_code_with)
)
order by date_update
SEGMENTED BY hash(date_update, currency_code, currency_code_with) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 90, 60);