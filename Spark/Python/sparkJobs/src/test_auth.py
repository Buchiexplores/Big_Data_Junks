
import configparser

config = configparser.ConfigParser()
config.read('/home/fieldemployee/Big_Data_Training/Spark/conf/db.ini')

#Get authentication details
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
postgresql_user  = config['postgresql']['user']
postgresql_password = config['postgresql']['password']
sqlserver_user = config['postgresql']['user']
sqlserver_password = config['sqlserver']['password']

print(type(mysql_user))
print(mysql_password)