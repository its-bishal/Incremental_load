
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql

from py4j.java_gateway import java_import
from pyspark.sql import DataFrame

# import os

# creating spark session
spark = SparkSession.builder \
    .appName('Incremental load') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:19000") \
    .getOrCreate()

# Import necessary Hadoop classes
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')



# creating sql connection to work with the database
connection = pymysql.connect(
    host='<host_name>',
    user='<username>',
    password='<your_password>',
    database='extenso_001_config'
)

# unlike in the incremental_data_load, the updated time of the table is compared with config table start and end_date
# and whole of the table is loaded as per the requirement and instructions
def upload():
    jdbc_url = f"jdbc:mysql://localhost:3306/extenso_001_config"
    connection_properties = {"user":"<username>", "password":"<your password>", "driver":"com.mysql.cj.jdbc.Driver"}

    data = spark.read.jdbc(url=jdbc_url, table='cf_etl_table2', properties=connection_properties)
    data.show()
    # data.show()

    for i in range(data.count()):
        
        # check if the datatable is incremental
        incremental = data.select('is_incremental').collect()
        if_incre =incremental[i]['is_incremental']
        print(if_incre)

        if if_incre==1:

            # pass the query as alias since the table function does not take a query as value
            query = "(SELECT * FROM extenso_001_config.cf_etl_table2 WHERE incremental_field BETWEEN start_date_time AND end_date_time) AS query_alias"

            df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

            name_schema = df.select('schemaName').collect()
            schema = name_schema[i]['schemaName']
            print(schema)

            name_table = df.select('tableName').collect()
            table = name_table[i]['tableName']
            print(table)

            hdfs_loc = df.select('location').collect()
            loc = hdfs_loc[i]['location']
            print(loc)

            hdfs = df.select('hdfs_file_name').collect()
            hdfs_file = hdfs[i]['hdfs_file_name']
            print(hdfs_file)

            hdfs_path = f"{loc}{hdfs_file}"

            jdbc_url = f"jdbc:mysql://localhost:3306/{schema}"
            connection_properties = {"user":"<username>", "password":"<your password>", "driver":"com.mysql.cj.jdbc.Driver"}


            new_df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)

            new_df.show()


            # Define HDFS path
            # Get the Hadoop FileSystem
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

            # Check if the file exists
            path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            if fs.exists(path):
                # print('file exists')
                new_df.write.mode('append').parquet(hdfs_path)
            else:
                new_df.write.mode('overwrite').parquet(hdfs_path)
                # print('file doesnot exist')

            with connection.cursor() as cursor:

                # set execution time to the current time
                exec_date_query = f"update `extenso_001_config`.cf_etl_table2 set execution_date = (current_timestamp) where id = {i+1}"
                cursor.execute(exec_date_query)

                # increase the date in start_date_time column by a day
                update_startdate_query = "update `extenso_001_config`.cf_etl_table2 set start_date_time = date_add(start_date_time, interval 1 day)"
                cursor.execute(update_startdate_query)

                # increase the date in end_date_time column by a day
                update_enddate_query = "update `extenso_001_config`.cf_etl_table2 set end_date_time = date_add(end_date_time, interval 1 day)"
                cursor.execute(update_enddate_query)

                connection.commit()

        elif if_incre==0:

            name_schema = data.select('schemaName').collect()
            schema = name_schema[i]['schemaName']
            print(schema)

            name_table = data.select('tableName').collect()
            table = name_table[i]['tableName']
            print(table)

            hdfs_loc = data.select('location').collect()
            loc = hdfs_loc[i]['location']
            print(loc)

            hdfs = data.select('hdfs_file_name').collect()
            hdfs_file = hdfs[i]['hdfs_file_name']
            print(hdfs_file)

            jdbc_url = f"jdbc:mysql://localhost:3306/{schema}"
            connection_properties = {"user":"<username>", "password":"<your password>", "driver":"com.mysql.cj.jdbc.Driver"}


            df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
            df.show()

            hdfs_path = f"{loc}{hdfs_file}"

            df.write.mode("overwrite").parquet(hdfs_path)


upload()