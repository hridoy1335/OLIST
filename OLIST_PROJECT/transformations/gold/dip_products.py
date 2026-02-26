from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="product_source")
def product_source():

    product = (
        spark.readStream.table("silver.products")
        .filter(col("product_id").isNotNull())
        .withColumn("ingest_at", col("ingest_at").cast("timestamp"))
    )

    product_category = (
        spark.read.table("silver.product_category")   # static lookup
        .filter(col("product_category_name").isNotNull())
    )

    dim_product = (
        product.alias("p")
        .join(
            product_category.alias("c"),
            col("p.product_category_name") == col("c.product_category_name"),
            "left"
        )
        .select(
            col("p.product_id"),
            col("p.product_category_name").alias("product_name"),
            col("c.product_category_name_english").alias("product_name_english"),
            col("p.ingest_at")
        )
    )

    return dim_product

dp.create_streaming_table(
    name="gold.dim_products",
    comment="products Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Create SCD Type 2 CDC flow
dp.create_auto_cdc_flow(
    target="gold.dim_products",
    source="product_source",
    keys=["product_id"],
    sequence_by="ingest_at", 
    stored_as_scd_type=2
)
