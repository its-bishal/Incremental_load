
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql


spark = SparkSession.builder \
    .appName("Selective Incremental Load") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:19000") \
    .getOrCreate()


# the row to be passed from the connection_value table
row_num =1 

# the concept is to load the data as specified by the incermental flag
# if the incremental flag is set to true, then data is added incrementally to the same hadoop location

# if incremental flag set to false, the data is overwritten at the specified location,
# meaning the file is created if not existing, if existing then replaced

# along with this the comparision dates i.e. start and end dates are incremented with each run
# allowing the data to be loaded each day



# this allows for the storage of config file into a database table connection value
# the config file contains the connection values including the host, user, password and config database
# prior version of the code were limited by the passing of such connection information in the execution file, 
# limiting to create changes with differentitation from the system or file locations

def selective_load():
    jdbc_url = f"jdbc:mysql://localhost:3306/extenso_001_config"
    connection_properties = {"user":"<username>", "password":"<your password>", "driver":"com.mysql.cj.jdbc.Driver"}

    conn_value = spark.read.jdbc(url=jdbc_url, table='connection_value', properties=connection_properties)
    extract_conn_value = (conn_value.filter(conn_value.id==row_num).select("db_host", "db_user", "pwd", "config_db", "jdbc_url")).collect()

    # Extract the single values
    host_db = extract_conn_value[row_num-1]['db_host']
    user = extract_conn_value[row_num-1]['db_user']
    password = extract_conn_value[row_num-1]['pwd']
    configdb = extract_conn_value[row_num-1]['config_db']
    jdbcurl = extract_conn_value[row_num-1]['jdbc_url']

    # a mysql connection is a must while executing sql queries from pyspark to sql databases
    connection = pymysql.connect(
        host='<host_name>',
        user='<username>',
        password='<your_password>',
        database='extenso_001_config'
    )

    # reading the tables from jdbc url which contains database connection
    data = spark.read.jdbc(url=jdbc_url, table='cf_etl_table3', properties=connection_properties)
    datamap = spark.read.jdbc(url=jdbc_url, table='cf_fields_mapping', properties=connection_properties)

    # looping to execute each table from different databases sequentially
    for j in range(data.count()):

        incremental = data.select('is_incremental').collect()
        if_incre = incremental[j]['is_incremental']

        new_id = data.select('id').collect()
        stable_id = new_id[j]['id']

        name_schema = data.select('schemaName').collect()
        schema = name_schema[j]['schemaName']
        print(schema)

        name_table = data.select('tableName').collect()
        table = name_table[j]['tableName']
        print(table)

        hdfs_loc = data.select('location').collect()
        loc = hdfs_loc[j]['location']
        print(loc)

        hdfs = data.select('hdfs_file_name').collect()
        hdfs_file = hdfs[j]['hdfs_file_name']
        print(hdfs_file)

        # hdfs location to where the file has to be uploaded
        jdbc_url1 = f"{jdbcurl}{schema}"

        fields_to_extract = (datamap.filter(datamap.etl_table_id == stable_id) \
            .select("source_field", "destination_field").collect())

        source_fields = [field['source_field'] for field in fields_to_extract]

        destination_fields = [field['destination_field'] for field in fields_to_extract]

        print(source_fields)
        print(destination_fields)

        if if_incre==1:

            dcol = data.select('incremental_field').collect()
            datecol = dcol[j]['incremental_field']
            print(datecol)

            sdate = data.select('start_date_time').collect()
            startdate = sdate[j]['start_date_time']
            print(startdate)

            edate = data.select('end_date_time').collect()
            enddate = edate[j]['end_date_time']
            print(enddate)

            hdfs_path = f"{loc}{hdfs_file}"

            select_query = f"(select {', '.join(source_fields)} from {schema}.{table} where {datecol} between '{startdate}' and '{enddate}') as alias_query"

            df = spark.read.jdbc(url=jdbc_url1, table=select_query, properties=connection_properties)

            for src, des in zip(source_fields, destination_fields):
                df = df.withColumnRenamed(src, des)
            df.show(3)


            # Get the Hadoop FileSystem
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

            # check for the existence of the file before appending or overwriting
            path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            if fs.exists(path):
                df.write.mode('append').parquet(hdfs_path)
            else:
                df.write.mode('overwrite').parquet(hdfs_path)

            # a cursor is necessary for running the sql queries and creating changes to the mysql database
            with connection.cursor() as cursor:

                # set execution time to the current time
                exec_date_query = f"update `extenso_001_config`.cf_etl_table2 set execution_date = (current_timestamp) where id = {j+1}"
                cursor.execute(exec_date_query)

                # increase the date in start_date_time column by a day
                update_startdate_query = "update `extenso_001_config`.cf_etl_table2 set start_date_time = date_add(start_date_time, interval 1 day)"
                cursor.execute(update_startdate_query)

                # increase the date in end_date_time column by a day
                update_enddate_query = "update `extenso_001_config`.cf_etl_table2 set end_date_time = date_add(end_date_time, interval 1 day)"
                cursor.execute(update_enddate_query)

                connection.commit()


        # if the data has to be appended in non-incremental way
        # i.e. uploading if non existing else overwriting
        elif if_incre==0:


            select_query = f"(select {', '.join(source_fields)} from {schema}.{table}) as alias_query1"

            df = spark.read.jdbc(url=jdbc_url1, table=select_query, properties=connection_properties)

            for src, des in zip(source_fields, destination_fields):
                df = df.withColumnRenamed(src, des)

            df.show(3)

            hdfs_path = f"{loc}{hdfs_file}"
            print(f"writing data to {hdfs_path}")

            df.write.mode("overwrite").parquet(hdfs_path)



selective_load()