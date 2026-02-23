from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.orders",
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"order_id_not_null": "order_id IS NOT NULL"})
@dp.expect_all_or_drop({"customer_id_not_null": "customer_id IS NOT NULL"})
@dp.expect_all_or_drop({"order_status_not_null": "order_status IS NOT NULL"})
@dp.expect_all_or_drop({"order_purchase_timestamp_not_null": "order_purchase_timestamp IS NOT NULL"})
@dp.expect_all_or_drop({"order_approved_at_not_null": "order_approved_at IS NOT NULL"})
@dp.expect_all_or_drop({"order_delivered_carrier_date_not_null": "order_delivered_carrier_date IS NOT NULL"})
@dp.expect_all_or_drop({"order_delivered_customer_date_not_null": "order_delivered_customer_date IS NOT NULL"})
@dp.expect_all_or_drop({"order_estimated_delivery_date_not_null": "order_estimated_delivery_date IS NOT NULL"})
def order_review_bronze():
    df = (
        spark.readStream.table("olist_cata.landing_data.orders")
        .withColumn("ingest_at", current_timestamp())
    )
    return df