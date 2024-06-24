drop table if exists STV2024031233__DWH.h_currency cascade;
create table if not exists STV2024031233__DWH.h_currency(
	hk_currency bigint not null,
	currency_code int not null,
	country varchar(100) not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_h_currency primary key(hk_currency),
	constraint uq_hk_currency_currency_cod unique(currency_code),
	constraint uq_hk_currency_country unique(country)
)
order by load_dt
SEGMENTED BY hk_currency all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);