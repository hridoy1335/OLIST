from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="sellers_source")
def sellers_source():
    # Seller table
    seller = (
        spark.readStream.table("silver.sellers")
        .filter(col("seller_id").isNotNull())
        .filter(col("seller_zip_code_prefix").isNotNull())
        .dropDuplicates(["seller_id"])
        .withColumn("ingest_at_seller", col("ingest_at").cast(TimestampType()))  # rename to avoid ambiguity
    )

    # Geolocation table
    geo_location = (
        spark.read.table("silver.geolocation")
        .filter(col("geolocation_zip_code_prefix").isNotNull())
        .withColumn("geolocation_city", upper(col("geolocation_city")))
        .dropDuplicates(["geolocation_zip_code_prefix"])
        # Optionally rename ingest_at if you need it
        .withColumnRenamed("ingest_at", "ingest_at_geo")
    )

    # Join seller with geolocation
    dim_seller = (
        seller.join(
            geo_location,
            seller.seller_zip_code_prefix == geo_location.geolocation_zip_code_prefix,
            "left"
        )
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "geolocation_city",
            "geolocation_state",
            col("ingest_at_seller").alias("ingest_at")  # final column for CDC
        )
    )

    return dim_seller

# Create target table
dp.create_streaming_table(
    name="gold.dim_seller",
    comment="Sellers Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Create SCD Type 2 CDC flow
dp.create_auto_cdc_flow(
    target="gold.dim_seller",
    source="sellers_source",
    keys=["seller_id"],
    sequence_by="ingest_at",  # now safe
    stored_as_scd_type=2
)