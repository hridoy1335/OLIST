from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="bronze_product_category_ingestion")
def bronze_product_category_ingestion():
    return (
        spark.readStream.table("bronze.product_category")
        .filter(col("product_category_name").isNotNull())
        .filter(col("product_category_name_english").isNotNull())
        .filter(col("ingest_at").isNotNull())
        .withColumn("product_category_name", upper(col("product_category_name")))
        .withColumn("product_category_name_english", upper(col("product_category_name_english")))
    )

dp.create_streaming_table(
    name="silver.product_category",
    schema="""
        product_category_name STRING, 
        product_category_name_english STRING,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="silver.product_category",
    source="bronze_product_category_ingestion",
    keys=["product_category_name", "product_category_name_english"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)