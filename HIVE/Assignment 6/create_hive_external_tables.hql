use raw;

CREATE EXTERNAL TABLE raw.genre_external(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/genre/';

CREATE EXTERNAL TABLE raw.subgenre_external (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/subgenre/';

CREATE EXTERNAL TABLE raw.year_external(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/year/';

CREATE EXTERNAL TABLE raw.albums_external(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/csv/';

