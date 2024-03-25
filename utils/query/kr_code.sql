Use stock;

create table kr_code
(
    표준코드 varchar(12),
    종목코드 varchar(6),
    primary key(종목코드)
);

SELECT * FROM kr_code;