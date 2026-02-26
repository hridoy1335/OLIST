from pyspark.sql.functions import *
from pyspark import pipelines as dp

# =========================================================
# 1️⃣ SOURCE VIEW (Streaming + Enrichment + Surrogate Key)
# =========================================================

@dp.view(name="customer_source")
def customer_source():
    # STREAMING source
    customer = (
        spark.readStream.table("silver.customers")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_city", upper(col("customer_city")))
    )

    # STATIC enrichment table
    geolocation = spark.read.table("silver.geolocation")

    geo = (
        geolocation
        .select(
            col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            col("geolocation_city").alias("geo_city"),
            col("geolocation_state").alias("geo_state")
        )
        .withColumn("geo_city", upper(col("geo_city")))
        .dropDuplicates(["zip_code_prefix"])
    )

    dim_customer = (
        customer
        .join(
            geo,
            customer.customer_zip_code_prefix == geo.zip_code_prefix,
            "left"
        )
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_city"),
            col("customer_state"),
            col("geo_city"),
            col("geo_state"),
            col("ingest_at")  # sequence column for SCD2
        )
    )

    # Add surrogate key
    return dim_customer


# =========================================================
# 2️⃣ CREATE TARGET STREAMING TABLE
# =========================================================

dp.create_streaming_table(
    name="gold.dim_customer",
    comment="Customer Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)


# =========================================================
# 3️⃣ APPLY SCD TYPE 2
# =========================================================

dp.create_auto_cdc_flow(
    target="gold.dim_customer",
    source="customer_source",
    keys=["customer_id"],     # business key
    sequence_by="ingest_at",  # ✅ pass column name as string
    stored_as_scd_type=2      # SCD Type 2
)