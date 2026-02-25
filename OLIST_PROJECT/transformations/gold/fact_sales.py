from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.view(name="fact_sales_source")
def fact_sales_source():

    order_items = spark.readStream.table("silver.order_items")
    orders = spark.read.table("silver.orders")
    payments = spark.read.table("silver.payments")
    products = spark.read.table("silver.products")

    return (
        order_items.alias("oi")
        .join(
            orders.alias("o"),
            col("oi.order_id") == col("o.order_id"),
            "left"
        )
        .join(
            payments.alias("p"),
            col("oi.order_id") == col("p.order_id"),
            "left"
        )
        .join(
            products.alias("pr"),
            col("oi.product_id") == col("pr.product_id"),
            "left"
        )
        .select(
            col("oi.order_id"),
            col("o.customer_id"),
            col("oi.order_item_id"),
            col("oi.product_id"),
            col("oi.seller_id"),
            col("oi.price"),
            col("oi.freight_value"),
            (col("oi.price") + col("oi.freight_value")).alias("total_price"),
            col("p.payment_sequential"),
            col("p.payment_value"),
            col("pr.product_name_length"),
            col("pr.product_description_length"),
            col("pr.product_photos_qty"),
            col("pr.product_weight_g"),
            col("pr.product_weight_kg"),
            col("pr.product_length_cm"),
            col("pr.product_height_cm"),
            col("pr.product_width_cm"),
            col("pr.product_volume_cm3"),
            current_timestamp().alias("fact_ingest_at")
        )
    )


dp.create_streaming_table(
    name="gold.fact_sales",
    comment="Fact Sales Table",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="gold.fact_sales",
    source="fact_sales_source",
    keys=["order_item_id"],
    sequence_by="fact_ingest_at", 
    stored_as_scd_type=1
)