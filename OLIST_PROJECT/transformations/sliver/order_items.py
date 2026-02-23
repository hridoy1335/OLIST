from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.view(name="bronze_order_items_ingestion")
def bronze_order_items_ingestion():
    return (
        spark.readStream.table("bronze.order_items")
        .withColumn("total_price", col("price") + col("freight_value"))
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "total_price",
            "ingest_at"
        )
    )


dp.create_streaming_table(
    name="silver.order_items",
    schema="""
        order_id STRING,
        order_item_id BIGINT,
        product_id STRING,
        seller_id STRING,
        shipping_limit_date TIMESTAMP,
        price DOUBLE,
        freight_value DOUBLE,
        total_price DOUBLE,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)


dp.create_auto_cdc_flow(
    target="silver.order_items",
    source="bronze_order_items_ingestion",
    keys=["order_id", "order_item_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)