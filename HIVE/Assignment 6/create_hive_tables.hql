use musicdb;

CREATE EXTERNAL TABLE genre_external(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/genre/';

CREATE EXTERNAL TABLE subgenre_external (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/subgenre/';

CREATE EXTERNAL TABLE year_external(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/year/';

CREATE EXTERNAL TABLE albums_external(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/csv/';

CREATE TABLE genre_internal(
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/genre/';

CREATE TABLE subgenre_internal (
    id VARCHAR(1000),
    labels VARCHAR(1000),
    Names VARCHAR(255))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/subgenre/';

CREATE TABLE year_internal(
    id VARCHAR(255),
    label VARCHAR(255),
    name int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/postgresql/musicdb/year/';

CREATE TABLE albums_internal(
    id VARCHAR(255),
    label VARCHAR(255),
    title VARCHAR(10000),
    year INT,
    number INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/databases/csv/';


