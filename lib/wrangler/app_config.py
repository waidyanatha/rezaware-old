''' 
    WRANGLER DEPLOYMENT SETTINGS
    
    should be set to suppport the class libraries and the application
    to use the deplyment specific paramters for the wrangler work loads
    
    See the respective sections: Implementation, Security, Database, Spark
        Timezon, Currency
'''

''' IMPLEMENTATION '''
#--application hosting location must be replaced with deployed url 
#  default localhost
host_ip = "127.0.0.1"
#--data hosting root location or S3 Bucket/Object
#  default ../data/
data = "../data/"

''' SECURITY '''
#--AWS security key and 
aws_security_key = ""
aws_security_token = ""
aws_security_policy = ""
aws_security_IAM = ""

''' DATABASE '''
#--database types: mysql, postgresql (default: postgres)
db_type = "postgresql"
#--port default 5432
db_port = "5432"
#--database driver
#  postgresql: 'org.postgresql.Driver'
db_driver = "org.postgresql.Driver"
#--database name
db_name = "rezstage"
#--schema name
db_schema = "lakehouse"
#--username and password to connect
#  default db_user="postgres", db_pswd = "postgres"
db_user = "nuwan"
db_pswd = "kotchchi7281"

''' SPARK '''
#--settings to connect to the database to perform work loads '''
#  install and setup spark: https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
#  also install findspark by running >>> python3 -m pip install findspark
#  to download Postgres JDBC drivers: https://jdbc.postgresql.org/
spark_home = '/opt/spark/'
spark_bin = '/opt/spark/bin'
spark_jar = '/opt/spark/jars/postgresql-42.5.0.jar'

''' TIMEZONE '''
#-- implementation specific timezone in UTC
#   default set to UTC 0:00
utc = "-8:00"

''' CURRENCY '''
#-- default currency for the implementation
currency = "USD"