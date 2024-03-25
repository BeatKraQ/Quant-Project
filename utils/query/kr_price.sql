use stock;

create table kr_price
(
	날짜 date,
    시가 double, 
    고가 double,
    저가 double,
    종가 double,
    거래량 double,
    종목코드 varchar(6),
    primary key(날짜, 종목코드)
);

select * from kr_price;