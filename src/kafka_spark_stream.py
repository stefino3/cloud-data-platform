from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, trim, when, concat_ws, lit, row_number
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType
)
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CLEAN_PATH = str(PROJECT_ROOT / "data/delta/stream_clean")
REJECTED_PATH = str(PROJECT_ROOT / "data/delta/stream_rejected")
CURRENT_STATE_PATH = str(PROJECT_ROOT / "data/delta/current_sales_state")

CLEAN_CHECKPOINT = str(PROJECT_ROOT / "data/checkpoints/delta_clean")
REJECTED_CHECKPOINT = str(PROJECT_ROOT / "data/checkpoints/delta_rejected")
CURRENT_STATE_CHECKPOINT = str(
    PROJECT_ROOT / "data/checkpoints/current_sales_state"
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sales-events"

def create_spark():

    spark = (
        SparkSession.builder
        .appName("KafkaSalesPipeline")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.log.level", "ERROR")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark

def read_kafka_stream(spark):

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
    )

def parse_events(kafka_df):

    event_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DecimalType(18, 2), True)
    ])

    return (
        kafka_df
        .select(
            col("value").cast("string").alias("raw_value"),
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
        .withColumn(
            "event",
            from_json(col("raw_value"), event_schema)
        )
        .select(
            "raw_value",
            col("event.id").alias("id"),
            col("event.name").alias("name"),
            col("event.amount").alias("amount"),
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
    )


def validate_events(parsed_df):

    return (
        parsed_df
        .withColumn(
            "error",
            concat_ws(
                "; ",

                when(
                    col("id").isNull(),
                    lit("ID must be a valid integer")
                ),

                when(
                    col("id").isNotNull() & (col("id") <= 0),
                    lit("ID must be a positive integer")
                ),

                when(
                    col("name").isNull() | (trim(col("name")) == ""),
                    lit("Name must not be empty")
                ),

                when(
                    col("amount").isNull(),
                    lit("Amount must be a valid decimal number")
                ),

                when(
                    col("amount").isNotNull() & (col("amount") < 0),
                    lit("Amount must not be negative")
                )
            )
        )
    )


def split_events(validated_df):

    clean_df = validated_df.filter(
        col("error") == ""
    )

    rejected_df = validated_df.filter(
        col("error") != ""
    )

    return clean_df, rejected_df

def merge_current_state(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_spark = batch_df.sparkSession

    latest_df = (
        batch_df
        .withColumn(
            "_row_number",
            row_number().over(
                Window
                .partitionBy("id")
                .orderBy(
                    col("timestamp").desc(),
                    col("offset").desc()
                )
            )
        )
        .filter(col("_row_number") == 1)
        .select(
            "id",
            "name",
            "amount",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
    )

    if not DeltaTable.isDeltaTable(
        batch_spark,
        CURRENT_STATE_PATH
    ):
        (
            latest_df.write
            .format("delta")
            .mode("overwrite")
            .save(CURRENT_STATE_PATH)
        )

    else:
        delta_table = DeltaTable.forPath(
            batch_spark,
            CURRENT_STATE_PATH
        )

        (
            delta_table.alias("target")
            .merge(
                latest_df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedUpdateAll(
                condition="source.offset > target.offset"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

event_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DecimalType(18, 2), True)
])

def main():

    spark = create_spark()

    print("Spark version:", spark.version)

    kafka_df = read_kafka_stream(spark)

    parsed_df = parse_events(kafka_df)

    validated_df = validate_events(parsed_df)

    clean_df, rejected_df = split_events(validated_df)

    clean_query = (
        clean_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CLEAN_CHECKPOINT
        )
        .start(CLEAN_PATH)
    )

    rejected_query = (
        rejected_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            REJECTED_CHECKPOINT
        )
        .start(REJECTED_PATH)
    )

    current_state_query = (
        clean_df.writeStream
        .foreachBatch(merge_current_state)
        .option(
            "checkpointLocation",
            CURRENT_STATE_CHECKPOINT
        )
        .queryName("current_sales_state")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
