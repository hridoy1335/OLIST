from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="date_source")
def dim_date():
    orders = spark.readStream.table("silver.orders")
    return (
        orders
        .select(col("order_purchase_timestamp").alias("full_date"))
        .withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("week_of_year", weekofyear("full_date"))
        .withColumn("day_of_week", date_format("full_date", "E"))
        .withColumn("ingest_at", current_timestamp())
        .dropDuplicates(["date_key"])
    )

dp.create_streaming_table(
    name="gold.dim_date",
    comment="Date Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="gold.dim_date",
    source="date_source",
    keys=["date_key"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=2
)