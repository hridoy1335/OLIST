from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="orders_source")
def orders_source():
    return (
        spark.readStream.table("silver.orders")
        .select(
            col("order_id"),
            col("order_status"),
            col("order_purchase_timestamp"),
            col("order_approved_at"),
            col("order_delivered_carrier_date"),
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date"),
            col("ingest_at")
        )
    )

dp.create_streaming_table(
    name="gold.dim_orders",
    comment="Date Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="gold.dim_orders",
    source="orders_source",
    keys=["order_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=2
)