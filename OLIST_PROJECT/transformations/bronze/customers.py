from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.customers",                 # ✔ fixed typo
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"customer_id_not_null": "customer_id IS NOT NULL"})
def customer_bronze():
    df = (
        spark.readStream.table("olist_cata.landing_data.customers")
        .withColumn("ingest_at", current_timestamp())
    )
    return df
