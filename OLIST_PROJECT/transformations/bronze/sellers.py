from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.sellers",                 # ✔ fixed typo
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"seller_id_not_null": "seller_id IS NOT NULL"})
@dp.expect_all_or_drop({"seller_zip_code_prefix_not_null": "seller_zip_code_prefix IS NOT NULL"})
@dp.expect_all_or_drop({"seller_city_not_null": "seller_city IS NOT NULL"})
@dp.expect_all_or_drop({"seller_state_not_null": "seller_state IS NOT NULL"})
def bronze_sellers():
    df = (
        spark.readStream.table("olist_cata.landing_data.sellers")
        .withColumn("ingest_at", current_timestamp())
    )
    return df
