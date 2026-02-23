from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.products",                 # ✔ fixed typo
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"product_id_not_null": "product_id IS NOT NULL"})
@dp.expect_all_or_drop({"product_category_name_not_null": "product_category_name IS NOT NULL"})
def bronze_products():
    df = (
        spark.readStream.table("olist_cata.landing_data.products")
        .withColumn("ingest_at", current_timestamp())
    )
    return df
