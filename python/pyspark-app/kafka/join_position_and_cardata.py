from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_trunc
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def main():
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("JoinPositionCardata_Streaming") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.driver.port", "7078") \
        .config("spark.driver.host", "pyspark") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()

    position_schema = StructType([
        StructField("driver_number", IntegerType(), True),
        StructField("session_key", IntegerType(), True),
        StructField("meeting_key", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("position", IntegerType(), True)
    ])

    car_data_schema = StructType([
        StructField("driver_number", IntegerType(), True),
        StructField("session_key", IntegerType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("speed", IntegerType(), True),
        StructField("n_gear", IntegerType(), True),
        StructField("throttle", IntegerType(), True),
        StructField("brake", IntegerType(), True),
        StructField("drs", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("meeting_key", IntegerType(), True)
    ])

    df_position_kafka = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "position_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    df_position = df_position_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), position_schema).alias("data")) \
        .select("data.*")

    df_position = df_position.withColumn("ts", to_timestamp(col("date"))) \
        .withColumn("ts_trunc", date_trunc("second", col("ts"))) \
        .withWatermark("ts", "1 minute")

    df_car_kafka = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "car_data_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    df_car = df_car_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), car_data_schema).alias("data")) \
        .select("data.*")

    df_car = df_car.withColumn("ts", to_timestamp(col("date"))) \
        .withColumn("ts_trunc", date_trunc("second", col("ts"))) \
        .withWatermark("ts", "1 minute")

    deduped_car = df_car.dropDuplicates(["driver_number", "session_key", "meeting_key", "ts_trunc"])

    joined_df = df_position.join(
        deduped_car,
        (df_position.driver_number == deduped_car.driver_number) &
        (df_position.session_key == deduped_car.session_key) &
        (df_position.meeting_key == deduped_car.meeting_key) &
        (df_position.ts_trunc == deduped_car.ts_trunc),
        "inner"
    )

    result_df = joined_df.select(
        df_position.driver_number,
        df_position.session_key,
        df_position.meeting_key,
        to_timestamp(df_position.date).alias("position_time"),
        df_position.position,
        deduped_car.speed
    )

    def write_to_postgres(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mydatabase") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "streaming.cardata_poc") \
            .option("user", "admin") \
            .option("password", "admin_password") \
            .mode("append") \
            .save()

    query = result_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/tmp/checkpoint_cardata_poc") \
        .start()

    print("Stream iniciado...")

    query.awaitTermination()

if __name__ == "__main__":
    main()
