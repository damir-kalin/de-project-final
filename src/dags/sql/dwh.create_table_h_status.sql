drop table if exists STV2024031233__DWH.h_status cascade;
create table if not exists STV2024031233__DWH.h_status(
	hk_status bigint not null,
	status varchar(50) not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_h_status primary key(hk_status),
	constraint uq_h_status_status unique(status)
)
order by load_dt
SEGMENTED BY hk_status all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);