from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.table(
    name="bronze.geolocation",
    table_properties={"quality": "bronze"}
)
@dp.expect_all_or_drop({"geolocation_zip_code_prefix": "geolocation_zip_code_prefix IS NOT NULL"})
@dp.expect_all_or_drop({"geolocation_lat": "geolocation_lat IS NOT NULL"})
@dp.expect_all_or_drop({"geolocation_lng": "geolocation_lng IS NOT NULL"})
@dp.expect_all_or_drop({"geolocation_city": "geolocation_city IS NOT NULL"})
@dp.expect_all_or_drop({"geolocation_state": "geolocation_state IS NOT NULL"})
def bronze_geolocation():
    df = (
        spark.readStream.table("olist_cata.landing_data.geolocation")
        .withColumn("ingest_at", current_timestamp())
    )
    return df