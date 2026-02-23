from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.order_items",  
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"order_id_not_null": "order_id IS NOT NULL"})
@dp.expect_all_or_drop({"order_item_id_not_null": "order_item_id IS NOT NULL"})
@dp.expect_all_or_drop({"product_id_not_null": "product_id IS NOT NULL"})
@dp.expect_all_or_drop({"seller_id_not_null": "seller_id IS NOT NULL"})
@dp.expect_all_or_drop({"shipping_limit_date_not_null": "shipping_limit_date IS NOT NULL"})
@dp.expect_all_or_drop({"price_not_null": "price IS NOT NULL AND price > 0"})
@dp.expect_all_or_drop({"freight_value_not_null": "freight_value IS NOT NULL AND freight_value >= 0"})
def order_items_bronze():
    df = (
        spark.readStream.table("olist_cata.landing_data.order_items")
        .withColumn("ingest_at", current_timestamp())
    )
    return df
