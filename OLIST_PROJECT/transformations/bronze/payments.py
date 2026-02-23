from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.payments",
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"order_id_not_null": "order_id IS NOT NULL"})
@dp.expect_all_or_drop({"payment_sequential_not_null": "payment_sequential IS NOT NULL"})
@dp.expect_all_or_drop({"payment_installments_not_null": "payment_installments IS NOT NULL"})
@dp.expect_all_or_drop({"payment_value_positive": "payment_value > 0"})
def bronze_payments():
    df = (
        spark.readStream.table("olist_cata.landing_data.payments")
        .withColumn("ingest_at", current_timestamp())
    )
    return df