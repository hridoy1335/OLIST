from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="bronze_customers_ingestion")
def bronze_customers_ingestion():
    return (
        spark.readStream.table("bronze.customers")
        .filter(col("customer_id").isNotNull())
    )

dp.create_streaming_table(
    name="silver.customers",
    schema="""
        customer_id STRING,
        customer_unique_id STRING,
        customer_zip_code_prefix BIGINT,
        customer_city STRING,
        customer_state STRING,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="silver.customers",
    source="bronze_customers_ingestion",
    keys=["customer_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)