from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, concat_ws, lit, round
from pyspark.sql.types import StructType, StructField, StringType


spark = (
    SparkSession.builder
    .appName("CloudDataCourse")
    .master("local[*]")
    .getOrCreate()
)

raw_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("amount", StringType(), True)
])

df = (
    spark.read
    .option("header", True)
    .schema(raw_schema)
    .csv("data/sales.csv")
)

raw_df = (
    df
    .withColumnRenamed("id", "id_raw")
    .withColumnRenamed("name", "name_raw")
    .withColumnRenamed("amount", "amount_raw")
)

parsed_df = (
    raw_df

    .withColumn(
        "id_value",
        when(
            col("id_raw").rlike(r"^[+-]?[0-9]+$"),
            col("id_raw").cast("long")
        )
    )

    .withColumn(
        "amount_value",
        when(
            col("amount_raw").rlike(r"^[+-]?[0-9]+(\.[0-9]+)?$"),
            col("amount_raw").cast("decimal(18,2)")
        )
    )
)

validated_df = parsed_df.withColumn(
    "error",
    concat_ws(
        "; ",

        when(
            col("id_value").isNull(),
            "ID must be a valid integer"
        ),

        when(
            col("id_value").isNotNull() & (col("id_value") <= 0),
            "ID must be a positive integer"
        ),

        when(
            col("name_raw").isNull() | (trim(col("name_raw")) == ""),
            "Name must not be empty"
        ),

        when(
            col("amount_value").isNull(),
            "Amount must be a valid decimal number"
        ),

        when(
            col("amount_value").isNotNull() & (col("amount_value") < 0),
            "Amount must not be negative"
        )
    )
)

clean_df = (
    validated_df
    .filter(col("error") == "")
    .withColumn(
        "vat",
        round(
            col("amount_value") * lit(Decimal("0.20")),
            2
        )
    )
    .withColumn(
        "amount_with_vat",
        round(
            col("amount_value") + col("vat"),
            2
        )
    )
)

clean_df = clean_df.select(
    col("id_value").alias("id"),
    trim(col("name_raw")).alias("name"),
    col("amount_value").alias("amount"),
    "vat",
    "amount_with_vat"
)

rejected_df = (
    validated_df
    .filter(col("error") != "")
    .select(
        col("id_raw").alias("id"),
        col("name_raw").alias("name"),
        col("amount_raw").alias("amount"),
        "error"
    )
)

print("\nFINAL CLEAN DATA:")
clean_df.show(truncate=False)

print("\nFINAL CLEAN SCHEMA:")
clean_df.printSchema()

print("\nFINAL REJECTED DATA:")
rejected_df.show(truncate=False)

clean_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("data/spark/clean")

rejected_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("data/spark/rejected")

clean_df.write \
    .mode("overwrite") \
    .parquet("data/parquet/clean")

rejected_df.write \
    .mode("overwrite") \
    .parquet("data/parquet/rejected")

raw_df.show()

parquet_df = spark.read.parquet(
    "data/parquet/clean"
)

selected_df = parquet_df.select(
    "name",
    "amount"
)

selected_df.explain(mode="formatted")

print("\nPARQUET DATA:")
parquet_df.show()

print("\nPARQUET SCHEMA:")
parquet_df.printSchema()

spark.stop()