from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="bronze_payments_ingestion")
def bronze_payments_ingestion():
    return (
        spark.readStream.table("bronze.payments")
        .filter(col("order_id").isNotNull())
        .withColumn("payment_type", upper(col("payment_type")))
        .withColumn("total_amount", col("payment_installments") * col("payment_value"))
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            "total_amount",
            "ingest_at"
        )
    )

dp.create_streaming_table(
    name="silver.payments",
    schema="""
        order_id STRING,
        payment_sequential BIGINT,
        payment_type STRING,
        payment_installments BIGINT,
        payment_value DOUBLE,
        total_amount DOUBLE,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="silver.payments",
    source="bronze_payments_ingestion",
    keys=["order_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)
