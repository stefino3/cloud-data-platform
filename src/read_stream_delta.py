from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("ReadStreamingDelta")
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


print("\nCLEAN DELTA DATA:")

clean_df = (
    spark.read
    .format("delta")
    .load("data/delta/stream_clean")
    .orderBy("offset")
)

clean_df.show(truncate=False)


print("\nREJECTED DELTA DATA:")

rejected_df = (
    spark.read
    .format("delta")
    .load("data/delta/stream_rejected")
    .orderBy("offset")
)

rejected_df.show(truncate=False)

print("\nCURRENT SALES STATE:")

current_df = (
    spark.read
    .format("delta")
    .load("data/delta/current_sales_state")
    .orderBy("id")
)

current_df.show(truncate=False)

spark.stop()