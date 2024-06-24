drop table if exists STV2024031233__DWH.h_operation cascade;
create table if not exists STV2024031233__DWH.h_operation(
	hk_operation bigint not null,
	operation_id uuid not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_h_operation primary key(hk_operation),
	constraint uq_h_operation_operation_id unique(operation_id)
)
order by load_dt
SEGMENTED BY hk_operation all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);