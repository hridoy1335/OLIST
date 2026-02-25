from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="payments_source")
def payments_source():
    return (
        spark.readStream.table("silver.payments")
        .select(
            col("order_id"),
            col("payment_sequential"),
            col("payment_type"),
            col("payment_installments"),
            col("ingest_at")
        )
    )

dp.create_streaming_table(
    name="gold.dim_payments",
    comment="Payments Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="gold.dim_payments",
    source="payments_source",
    keys=["order_id"],
    sequence_by="ingest_at", 
    stored_as_scd_type=2
)