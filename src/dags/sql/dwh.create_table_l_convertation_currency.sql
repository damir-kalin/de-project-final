drop table if exists STV2024031233__DWH.l_convertation_currency cascade;
create table if not exists STV2024031233__DWH.l_convertation_currency(
	hk_l_convertation_currency bigint not null,
	hk_currency bigint not null,
	hk_currency_with bigint not null,
	date_update timestamp not null,
	currency_with_div numeric(18, 2) not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_l_convertation_currency primary key(hk_l_convertation_currency),
	constraint uq_l_convertation_currency unique(hk_currency, hk_currency_with, date_update),
	constraint fk_l_convertation_currency_hk_currency foreign key(hk_currency) references STV2024031233__DWH.h_currency(hk_currency),
	constraint fk_l_convertation_currency_hk_currency_with foreign key(hk_currency_with) references STV2024031233__DWH.h_currency(hk_currency)
)
order by load_dt
SEGMENTED BY hk_l_convertation_currency all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);