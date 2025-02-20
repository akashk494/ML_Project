import logging
import mysql.connector
from mysql.connector import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS created_users (
        id VARCHAR(36) PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR(10),
        address TEXT,
        post_code VARCHAR(20),
        email VARCHAR(255),
        username VARCHAR(255),
        registered_date VARCHAR(50),
        phone VARCHAR(20),
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(cursor, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        cursor.execute("""
            INSERT INTO created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Error as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='stream',  # Replace with your database name
            user='root',            # Replace with your MySQL username
            password='manager'         # Replace with your MySQL password
        )

        if connection.is_connected():
            return connection
    except Error as e:
        logging.error(f"Could not create MySQL connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        connection = create_mysql_connection()

        if connection is not None:
            cursor = connection.cursor()
            create_table(cursor)

            logging.info("Streaming is being started...")

            def foreach_batch_function(df, epoch_id):
                for row in df.collect():
                    insert_data(cursor, **row.asDict())
                connection.commit()  # Commit the transaction

            streaming_query = (selection_df.writeStream
                               .foreachBatch(foreach_batch_function)
                               .start())

            streaming_query.awaitTermination()

            cursor.close()
            connection.close()
