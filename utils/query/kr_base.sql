use stock;

create table kr_base
(
	종목코드 varchar(6) not null,
    종목명 varchar(20),
    시장구분 varchar(6),
    종가 float,
    시가총액 float,
    기준일 date,
    EPS float,
    선행EPS float,
    BPS float,
    주당배당금 float,
    종목구분 varchar(5),
    primary key(종목코드, 기준일)
    );

select * from kr_base;