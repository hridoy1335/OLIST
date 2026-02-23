from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="bronze_geolocation_ingestion")
def bronze_geolocation_ingestion():
    return (
        spark.readStream.table("bronze.geolocation")
        .filter(col("geolocation_zip_code_prefix").isNotNull())
        .withColumn("ingest_at", current_timestamp())  # <-- ADD THIS
    )

dp.create_streaming_table(
    name="silver.geolocation",
    schema="""
        geolocation_zip_code_prefix BIGINT,
        geolocation_lat DOUBLE,
        geolocation_lng DOUBLE,
        geolocation_city STRING,
        geolocation_state STRING,
        ingest_at TIMESTAMP NOT NULL
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="silver.geolocation",
    source="bronze_geolocation_ingestion",
    keys=["geolocation_zip_code_prefix"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)