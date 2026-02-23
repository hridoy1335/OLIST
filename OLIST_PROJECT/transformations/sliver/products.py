from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="bronze_products_ingestion")
def bronze_products_ingestion():
    return (
        spark.readStream.table("bronze.products")
        .filter(col("product_id").isNotNull())
        .filter(col("product_category_name").isNotNull())
        .withColumn("product_category_name", upper(col("product_category_name")))
        # Null-safe Volume in cm³
        .withColumn(
            "product_volume_cm3",
            col("product_length_cm").cast("BIGINT") *
            col("product_height_cm").cast("BIGINT") *
            col("product_width_cm").cast("BIGINT")
        )
        # Weight in kg
        .withColumn("product_weight_kg", col("product_weight_g") / 1000)
        .withColumn("product_weight_g", col("product_weight_g").cast("bigint"))
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .select(
            "product_id",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_weight_kg",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_volume_cm3",
            "ingest_at"
        )
    )

# Create silver table
dp.create_streaming_table(
    name="silver.products",
    schema="""
        product_id STRING,
        product_category_name STRING,
        product_name_length BIGINT,
        product_description_length BIGINT,
        product_photos_qty BIGINT,
        product_weight_g BIGINT,
        product_weight_kg DOUBLE,
        product_length_cm BIGINT,
        product_height_cm BIGINT,
        product_width_cm BIGINT,
        product_volume_cm3 BIGINT,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Auto CDC flow (SCD Type 1)
dp.create_auto_cdc_flow(
    target="silver.products",
    source="bronze_products_ingestion",
    keys=["product_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)