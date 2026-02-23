from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.order_review",
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"review_id_not_null": "review_id IS NOT NULL"})
@dp.expect_all_or_drop({"order_id_not_null": "order_id IS NOT NULL"})
def order_review_bronze():
    df = (
        spark.readStream.table("olist_cata.landing_data.order_reviews")
        .withColumn("ingest_at", current_timestamp())
    )
    return df