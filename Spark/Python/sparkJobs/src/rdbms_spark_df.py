# Author: Abuchi Okeke
# Version: 0.0.4
# Date: 26/10/2020
# Description: A pyspark program for ingesting tables from Mysql, postgresql, sqlserver databases and csv files to hdfs

#import modules
from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext, HiveContext, SparkContext
import configparser

#set paths for driver connector jars
mysql_connector_path = "/usr/local/spark-2.4.7/jars/mysql-connector-java.jar"
postgresql_connector_path = "/usr/local/spark-2.4.7/jars/postgresql-connector-java.jar"
sqlserver_connector_path = "/usr/local/spark-2.4.7/jars/mssql-connector-java.jar"

#set authentication path
auth_path = '/home/fieldemployee/Big_Data_Training/Spark/conf/db.ini'

# Define databases servers to use
db1 = "mysql"
db2 = "postgresql"
db3 = "sqlserver"

# Get passwords
# mysql
# mysql_f = open('/home/fieldemployee/bin/passwords/mysql.password')
# mysql_password = mysql_f.readline()
# mysql_f.close()
# # postgresql
# postgresql_f = open('/home/fieldemployee/bin/passwords/postgres_local.password')
# postgresql_password = postgresql_f.readline()
# postgresql_f.close()

#Get authentication details for each of the databases server
config = configparser.ConfigParser()
config.read(auth_path)

mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
postgresql_user  = config['postgresql']['user']
postgresql_password = config['postgresql']['password']
sqlserver_user = config['postgresql']['user']
sqlserver_password = config['sqlserver']['password']


# Build spark session for each of the different RDBMS servers (mysql, sqlserver, postgresql)
# Mysql
spark_mysql = SparkSession.builder \
    .appName("Mysql_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", mysql_connector_path) \
    .getOrCreate()

# Postgresql
spark_postgresql = SparkSession.builder \
    .appName("Postgresql_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", postgresql_connector_path) \
    .getOrCreate()

# sqlServer
spark_sqlserver = SparkSession.builder \
    .appName("SqlServer_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", sqlserver_connector_path) \
    .getOrCreate()

# Create RDD for the different RDBMS using spark context
sc_mysql = spark_mysql.sparkContext
sc_postgresql = spark_postgresql.sparkContext
sc_sqlserver = spark_sqlserver.sparkContext

# Sql context connector for each of the servers
sqlc_mysql = SQLContext(sc_mysql)
sqlc_postgresql = SQLContext(sc_postgresql)
sqlc_sqlserver = SQLContext(sc_sqlserver)

# Hadoop Path
# path = "hdfs://localhost:9000/user/input/"
path = "/user/input"

#local file path
#path = "file:///home/fieldemployee/spark"


# Dataframe functions for each of the databases
def mysql_dataframe(user, password, table):
    mysql_df = spark_mysql.read \
        .format("jdbc") \
        .option('url', 'jdbc:mysql://localhost:3306/musicbrainz') \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    mysql_df.write.format("csv").save(path + "/mysql/" + table)


def postgresql_dataframe(user, password, table):
    postgresql_df = spark_postgresql.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:5432/musicbrainz') \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    postgresql_df.write.format("csv").save(path + "/postgresql/" + table)


def sqlserver_dataframe(user, password, table):
    sqlserver_df = spark_sqlserver.read \
        .format("jdbc") \
        .option('url', 'jdbc:sqlserver://localhost:1433;databaseName=master') \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    sqlserver_df.write.format("csv").save(path + "/sqlserver/" + table)

table =""
# Ingest Tables
with open("/home/fieldemployee/PycharmProjects/sparkJobs/src/tables.txt") as tables:
    for line in tables:
        #db, tb = line.partition("=")[::2]
        db, tb = str(line).split("=")
        # mysql
        if db.lower() == db1:
            table = tb.rstrip('\n')
            mysql_dataframe(mysql_user, mysql_password, table)

        # postgresql
        if db.lower() == db2:
            table = tb.rstrip('\n')
            postgresql_dataframe(postgresql_user, postgresql_password, table)

        # sqlserver
        if db.lower() == db3:
            table = tb.rstrip('\n')
            sqlserver_dataframe(sqlserver_user, sqlserver_password, table)

# Get CSV files
#set datasets path
dataset_path = "file:///home/fieldemployee/bin/datasets/"
spark_csv = SparkSession.builder.appName("CSV_DF").master("local[*]").getOrCreate()
csv_opensource_df = spark_csv.read \
    .format("csv").option("header", "true") \
    .option("inferSchema", "true") \
    .load(dataset_path + "opensource.csv")
csv_opensource_df.write.format("csv").save(path + "/csv/opensource")

csv_spotify_df = spark_csv.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load(dataset_path + "spotify.csv")
csv_spotify_df.write.format("csv").save(path + "/csv/spotify_api")

# df.coalesce(1).write.format('com.databricks.spark.csv').save('path+my.csv',header = 'true')
