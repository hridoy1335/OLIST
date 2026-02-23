from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.view(name="bronze_orders_ingestion")
def bronze_orders_ingestion():
    return (
        spark.readStream.table("bronze.orders")
        .filter(col("order_id").isNotNull())
        .withColumn("order_status", upper(col("order_status")))
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "ingest_at"
        )
    )


dp.create_streaming_table(
    name="silver.orders",
    schema="""
        order_id STRING,
        customer_id STRING,
        order_status STRING,
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)


dp.create_auto_cdc_flow(
    target="silver.orders",
    source="bronze_orders_ingestion",
    keys=["order_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)