from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.view(name="review_source")
def review_source():
    return (
        spark.readStream.table("silver.order_review")
        .select(
            col("order_id"),
            col("review_id"),
            col("review_score"),
            col("rating"),
            col("review_comment_message"),
            col("review_creation_date"),
            col("review_answer_timestamp"),
            col("ingest_at")
        )
    )

dp.create_streaming_table(
    name="gold.dim_review",
    comment="Review Dimension - SCD Type 2",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

dp.create_auto_cdc_flow(
    target="gold.dim_review",
    source="review_source",
    keys=["review_id"],
    sequence_by="ingest_at", 
    stored_as_scd_type=2
)