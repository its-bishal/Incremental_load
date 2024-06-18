

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql

spark = SparkSession.builder \
    .appName('Incremental load') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:19000") \
    .getOrCreate()


connection = pymysql.connect(
    host='<host_name>',
    user='<username>',
    password='<your_password>',
    database='extenso_001_config'
)


# the concept is to load the data as specified by the incermental flag
# if the incremental flag is set to true, then data is added incrementally to the same hadoop location

# if incremental flag set to false, the data is overwritten at the specified location,
# meaning the file is created if not existing, if existing then replaced

# along with this the comparision dates i.e. start and end dates are incremented with each run
# allowing the data to be loaded each day

# here each data inside the table is compared to the stard and end date
# and worked with accordingly
def upload():
    jdbc_url = f"jdbc:mysql://localhost:3306/extenso_001_config"
    connection_properties = {"user":"<username>", "password":"<your password>", "driver":"com.mysql.cj.jdbc.Driver"}

    data = spark.read.jdbc(url=jdbc_url, table='cf_etl_table3', properties=connection_properties)
    data.show()
    # data.show()

    for i in range(data.count()):
        
        # check if the datatable is incremental
        incremental = data.select('is_incremental').collect()
        if_incre =incremental[i]['is_incremental']
        print(if_incre)

        if if_incre==1:

            sdate = data.select('start_date_time').collect()
            startdate  = sdate[i]['start_date_time']
            print(startdate)

            edate = data.select('end_date_time').collect()
            enddate = edate[i]['end_date_time']
            print(enddate)

            name_schema = data.select('schemaName').collect()
            schema = name_schema[i]['schemaName']
            print(schema)

            name_table = data.select('tableName').collect()
            table = name_table[i]['tableName']
            print(table)

            dcol = data.select('incremental_field').collect()
            datecol = dcol[i]['incremental_field']
            print(datecol)

            hdfs_loc = data.select('location').collect()
            loc = hdfs_loc[i]['location']
            print(loc)

            hdfs = data.select('hdfs_file_name').collect()
            hdfs_file = hdfs[i]['hdfs_file_name']
            print(hdfs_file)
            
            hdfs_path = f"{loc}{hdfs_file}"
            print(hdfs_path)

            jdbc_url = f"jdbc:mysql://localhost:3306/{schema}"

            query = f"(SELECT * FROM {schema}.{table} WHERE {datecol} BETWEEN '{startdate}' AND '{enddate}') AS query_alias"
            df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

            df.show(5)

            # Get the Hadoop FileSystem
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

            path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            if fs.exists(path):
                # print('file exists')
                df.write.mode('append').parquet(hdfs_path)
            else:
                df.write.mode('overwrite').parquet(hdfs_path)
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

            df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
            df.show()

            hdfs_path = f"{loc}{hdfs_file}"

            df.write.mode("overwrite").parquet(hdfs_path)            



upload()