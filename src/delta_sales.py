from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pathlib import Path
from pyspark.sql.functions import input_file_name
from decimal import Decimal
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType
)


builder = (
    SparkSession.builder
    .appName("DeltaCourse")
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
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Spark version:", spark.version)

parquet_df = spark.read.parquet(
    "data/parquet/clean"
)

delta_path = str(
    Path("data/delta/clean_sales").resolve()
)

print("Delta path:", delta_path)

# parquet_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save(delta_path)
#
# print("\nDELTA DATA:")
#
# delta_df = spark.read \
#     .format("delta") \
#     .load(delta_path)
#
# delta_df.show()
#
# print("\nDELTA SCHEMA:")
# delta_df.printSchema()
#
# # spark.sql(f"""
# #     INSERT INTO delta.`{delta_path}`
# #     VALUES (
# #         4,
# #         'Diana',
# #         CAST(300.00 AS DECIMAL(18,2)),
# #         CAST(60.00 AS DECIMAL(20,2)),
# #         CAST(360.00 AS DECIMAL(22,2))
# #     )
# # """)
# #
# # print("\nDELTA DATA AFTER INSERT:")
#
# spark.read \
#     .format("delta") \
#     .load(delta_path) \
#     .show()
#
# print("\nVERSION 0:")
#
# version_0_df = (
#     spark.read
#     .format("delta")
#     .option("versionAsOf", 0)
#     .load(delta_path)
# )
#
# version_0_df.show()
#
#
# print("\nVERSION 1:")
#
# version_1_df = (
#     spark.read
#     .format("delta")
#     .option("versionAsOf", 1)
#     .load(delta_path)
# )
#
# version_1_df.show()
#
# print("\nDELTA HISTORY:")
#
# spark.sql(
#     f"DESCRIBE HISTORY delta.`{delta_path}`"
# ).select(
#     "version",
#     "timestamp",
#     "operation",
#     "operationParameters"
# ).show(truncate=False)
#
# # print("\nUPDATE DIANA:")
# #
# # spark.sql(f"""
# #     UPDATE delta.`{delta_path}`
# #     SET
# #         amount = CAST(350.00 AS DECIMAL(18,2)),
# #         vat = CAST(70.00 AS DECIMAL(20,2)),
# #         amount_with_vat = CAST(420.00 AS DECIMAL(22,2))
# #     WHERE id = 4
# # """)
#
# print("\nCURRENT DELTA DATA:")
#
# spark.read \
#     .format("delta") \
#     .load(delta_path) \
#     .orderBy("id") \
#     .show()
#
# print("\nDELTA HISTORY AFTER UPDATE:")
#
# spark.sql(
#     f"DESCRIBE HISTORY delta.`{delta_path}`"
# ).select(
#     "version",
#     "timestamp",
#     "operation",
#     "operationParameters"
# ).show(truncate=False)
#
# print("\nDIANA IN VERSION 1:")
#
# spark.read \
#     .format("delta") \
#     .option("versionAsOf", 1) \
#     .load(delta_path) \
#     .filter("id = 4") \
#     .show()
#
#
# print("\nDIANA IN VERSION 2:")
#
# spark.read \
#     .format("delta") \
#     .option("versionAsOf", 2) \
#     .load(delta_path) \
#     .filter("id = 4") \
#     .show()
#
# current_df = (
#     spark.read
#     .format("delta")
#     .load(delta_path)
# )
#
# print("\nCURRENT ACTIVE DATA WITH PARQUET FILE:")
#
# current_df \
#     .withColumn("parquet_file", input_file_name()) \
#     .orderBy("id") \
#     .show(truncate=False)
#
# print("\nCURRENT ACTIVE PARQUET FILES:")
#
# current_df \
#     .withColumn("parquet_file", input_file_name()) \
#     .select("parquet_file") \
#     .distinct() \
#     .show(truncate=False)

# print("\nDELETE BOB:")
#
# spark.sql(f"""
#     DELETE FROM delta.`{delta_path}`
#     WHERE id = 2
# """)
#
# print("\nCURRENT DATA AFTER DELETE:")
#
# current_after_delete_df = (
#     spark.read
#     .format("delta")
#     .load(delta_path)
#     .orderBy("id")
# )
#
# current_after_delete_df.show()
# ##############################################
# print("\nHISTORY AFTER DELETE:")
#
# history_df = spark.sql(
#     f"DESCRIBE HISTORY delta.`{delta_path}`"
# )
#
# (
#     history_df
#     .select(
#         "version",
#         "operation",
#         "operationParameters",
#         "operationMetrics"
#     )
#     .show(truncate=False)
# )
#
# incremental_schema = StructType([
#     StructField("id", LongType(), False),
#     StructField("name", StringType(), False),
#     StructField("amount", DecimalType(18, 2), False),
#     StructField("vat", DecimalType(20, 2), False),
#     StructField("amount_with_vat", DecimalType(22, 2), False)
# ])
#
# incremental_data = [
#     (
#         1,
#         "Alice",
#         Decimal("150.00"),
#         Decimal("30.00"),
#         Decimal("180.00")
#     ),
#     (
#         5,
#         "Edward",
#         Decimal("500.00"),
#         Decimal("100.00"),
#         Decimal("600.00")
#     )
# ]
#
# incremental_df = spark.createDataFrame(
#     incremental_data,
#     incremental_schema
# )
#
# print("\nINCREMENTAL BATCH:")
# incremental_df.show()
# #######################################
# delta_table = DeltaTable.forPath(
#     spark,
#     delta_path
# )
#
# (
#     delta_table.alias("target")
#     .merge(
#         incremental_df.alias("source"),
#         "target.id = source.id"
#     )
#     .whenMatchedUpdateAll()
#     .whenNotMatchedInsertAll()
#     .execute()
# )
####################################################
# print("\nDATA AFTER MERGE:")
#
# (
#     spark.read
#     .format("delta")
#     .load(delta_path)
#     .orderBy("id")
#     .show()
# )
# ###################################################
# print("\nHISTORY AFTER MERGE:")
#
# (
#     spark.sql(
#         f"DESCRIBE HISTORY delta.`{delta_path}`"
#     )
#     .select(
#         "version",
#         "operation",
#         "operationMetrics"
#     )
#     .show(truncate=False)
# )

# print("\nDETAIL BEFORE OPTIMIZE:")
#
# (
#     spark.sql(
#         f"DESCRIBE DETAIL delta.`{delta_path}`"
#     )
#     .select(
#         "numFiles",
#         "sizeInBytes"
#     )
#     .show(truncate=False)
# )
# ################################################
# print("\nRUNNING OPTIMIZE:")
#
# spark.sql(
#     f"OPTIMIZE delta.`{delta_path}`"
# ).show(truncate=False)
# ################################################
# print("\nDETAIL AFTER OPTIMIZE:")
#
# (
#     spark.sql(
#         f"DESCRIBE DETAIL delta.`{delta_path}`"
#     )
#     .select(
#         "numFiles",
#         "sizeInBytes"
#     )
#     .show(truncate=False)
# )
# ##############################################
# print("\nDATA AFTER OPTIMIZE:")
#
# (
#     spark.read
#     .format("delta")
#     .load(delta_path)
#     .orderBy("id")
#     .show()
# )
# #################################################
# print("\nHISTORY AFTER OPTIMIZE:")
#
# (
#     spark.sql(
#         f"DESCRIBE HISTORY delta.`{delta_path}`"
#     )
#     .select(
#         "version",
#         "operation",
#         "operationMetrics"
#     )
#     .show(truncate=False)
# )

partition_path = str(
    Path("data/delta/partition_demo").resolve()
)

# partition_data = [
#     (1, "Alice", "AT"),
#     (2, "Bob", "DE"),
#     (3, "Carla", "AT"),
#     (4, "Diana", "DE"),
#     (5, "Edward", "AT")
# ]
#
# partition_df = spark.createDataFrame(
#     partition_data,
#     ["id", "name", "country"]
# )
#
# print("\nPARTITION DEMO DATA:")
# partition_df.show()
#
# (
#     partition_df.write
#     .format("delta")
#     .mode("overwrite")
#     .partitionBy("country")
#     .save(partition_path)
# )
###############################################
# partitioned_df = (
#     spark.read
#     .format("delta")
#     .load(partition_path)
# )
#
# at_df = partitioned_df.filter("country = 'AT'")
#
# print("\nONLY AUSTRIA:")
# at_df.show()
#
# print("\nEXECUTION PLAN:")
# at_df.explain(mode="formatted")
################################################
# spark.sql(
#     f"OPTIMIZE delta.`{partition_path}`"
# ).show(truncate=False)

(
    spark.read
    .format("delta")
    .load(partition_path)
    .withColumn("parquet_file", input_file_name())
    .select("id","name","country","parquet_file")
    .distinct()
    .orderBy("id")
    .show(truncate=False)
)




spark.stop()