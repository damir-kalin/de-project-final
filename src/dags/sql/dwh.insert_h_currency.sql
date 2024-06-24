insert into STV2024031233__DWH.h_currency (hk_currency, currency_code, country, load_dt, load_src)
select distinct
hash(c.currency_code) as hk_currency,
c.currency_code,
t.country,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__STAGING.сurrencies as c
left join (
	select distinct
	l.currency_code,
	l.country 
	from  STV2024031233__STAGING.transactions as l ) as t on c.currency_code = t.currency_code
where c.currency_code not in (select p.currency_code from STV2024031233__DWH.h_currency as p)
	and c.date_update::date = '{{ ds }}'::date - interval '1 day'; 