use dsl;

CREATE TABLE dsl.genre_internal(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.genre_internal (select * from raw.genre_external);

CREATE TABLE dsl.subgenre_internal (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.subgenre_internal (select * from raw.subgenre_external);

CREATE TABLE dsl.year_internal(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.year_internal (select * from raw.year_external);

CREATE TABLE dsl.albums_internal(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into dsl.albums_internal (select * from raw.albums_external);

