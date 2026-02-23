from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.view(name="bronze_sellers_ingestion")
def bronze_sellers_ingestion():
    return (
        spark.readStream.table("bronze.sellers")
        .filter(col("seller_id").isNotNull())
        .filter(col("seller_zip_code_prefix") > 0)
        .withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast("bigint"))
        .withColumn("seller_city", upper(col("seller_city")))
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "ingest_at"
        )
    )


dp.create_streaming_table(
    name="silver.sellers",
    schema="""
        seller_id STRING,
        seller_zip_code_prefix BIGINT,
        seller_city STRING,
        seller_state STRING,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)


dp.create_auto_cdc_flow(
    target="silver.sellers",
    source="bronze_sellers_ingestion",
    keys=["seller_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)