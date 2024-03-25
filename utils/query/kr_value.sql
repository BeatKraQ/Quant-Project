use stock;

create table kr_value
(
종목코드 varchar(6),
기준일 date,
지표 varchar(3),
값 double,
primary key (종목코드, 기준일, 지표)
);

select * from kr_value;