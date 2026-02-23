from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.view(name="bronze_order_review_ingestion")
def bronze_order_review_ingestion():
    return (
        spark.readStream.table("bronze.order_review")
        .filter(col("review_id").isNotNull())
        .withColumn(
            "rating",
            when(col("review_score") > 4, "Excellent")
            .when(col("review_score") > 3, "Good")
            .when(col("review_score") > 2, "Average")
            .when(col("review_score") > 1, "Poor")
            .otherwise("Very Poor")
        )
        .select(
            "review_id",
            "order_id",
            "review_score",
            "rating",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            "ingest_at"
        )
    )


dp.create_streaming_table(
    name="silver.order_review",
    schema="""
        review_id STRING,
        order_id STRING,
        review_score BIGINT,
        rating STRING,
        review_comment_message STRING,
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP,
        ingest_at TIMESTAMP
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)


dp.create_auto_cdc_flow(
    target="silver.order_review",
    source="bronze_order_review_ingestion",
    keys=["review_id"],
    sequence_by=col("ingest_at"),
    stored_as_scd_type=1
)