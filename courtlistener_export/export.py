import os
import sys

import click
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import (
    coalesce,
    when,
    col,
    regexp_replace,
    concat_ws,
    collect_list,
    struct,
)


def parquetify(spark: SparkSession, in_path: str, out_path: str) -> None:
    """
    Conditionally converts a .csv.bz2 to parquet for faster processing and filtering.
    """
    csv_options = {
        "header": "true",
        "multiLine": "true",
        "quote": '"',
        "escape": '"',
    }
    if not os.path.exists(out_path):
        spark.read.options(**csv_options).csv(in_path).write.parquet(out_path)


def get_opinions(spark: SparkSession, path: str) -> DataFrame:
    """
    Loads opinions from parquet file and cleans up columns
    """
    text_col_priority = [
        "html_with_citations",
        "plain_text",
        "html",
        "html_lawbox",
        "html_columbia",
        "xml_harvard",
        "html_anon_2020",
    ]
    drop_cols = [
        "date_created",
        "date_modified",
        "joined_by_str",
        "sha1",
        "local_path",
        "extracted_by_ocr",
        "author_id",
        "opinion_id",
    ]
    drop_cols.extend(text_col_priority)
    return (
        spark.read.parquet(path)
        .alias("o")
        .withColumn("opinion_text", coalesce(*text_col_priority))
        .withColumn("opinion_text", regexp_replace("opinion_text", r"<.+?>", ""))
        .withColumn("page_count", col("page_count").cast(IntegerType()))
        .withColumn("ocr", when(col("extracted_by_ocr") == "t", True).otherwise(False))
        .withColumn("per_curiam", when(col("per_curiam") == "t", True).otherwise(False))
        .withColumn("cluster_id", col("cluster_id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def get_opinion_clusters(spark: SparkSession, path: str) -> DataFrame:
    """
    Loads opinion-clusters from parquet file and cleans up columns
    """
    drop_cols = [
        "date_created",
        "date_modified",
        "scdb_id",
        "scdb_decision_direction",
        "scdb_votes_majority",
        "scdb_votes_minority",
        "source",
        "procedural_history",
        "docket_id",  # todo join dockets?
    ]
    return (
        spark.read.parquet(path)
        .alias("oc")
        .withColumn("date_filed", col("date_filed").cast(DateType()))
        .withColumn(
            "date_filed_is_approximate",
            when(col("date_filed_is_approximate") == "t", True).otherwise(False),
        )
        .withColumn("date_blocked", col("date_blocked").cast(DateType()))
        .withColumn("blocked", when(col("blocked") == "t", True).otherwise(False))
        .withColumn("citation_count", col("citation_count").cast(IntegerType()))
        .withColumn("summary", regexp_replace("summary", r"<.+?>", ""))
        .withColumn("id", col("id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def get_citations(spark: SparkSession, path: str) -> DataFrame:
    """
    Loads citations from parquet file and cleans up columns
    """
    drop_cols = [
        "id",
        "volume",
        "reporter",
        "page",
        "type",  # might be needed. no current lookup table provided.
    ]
    return (
        spark.read.parquet(path)
        .alias("c")
        .withColumn("citation_text", concat_ws(" ", "volume", "reporter", "page"))
        .withColumn("cluster_id", col("cluster_id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def group(
    citations: DataFrame,
    opinions: DataFrame,
    opinion_clusters: DataFrame,
) -> DataFrame:
    """
    This groups the three datasets by cluster id and then merges them using
    reparent_opinions. There's a better way to do this in the pure dataframe
    api, but I didn't get around to figuring it out.
    """

    citations_arrays = citations.groupby(citations.cluster_id).agg(
        collect_list(struct(*[col(c).alias(c) for c in citations.columns]))
    )

    opinions_arrays = opinions.groupby(opinions.cluster_id).agg(
        collect_list(struct(*[col(c).alias(c) for c in opinions.columns]))
    )

    return opinion_clusters.join(
        citations_arrays, opinion_clusters.id == citations_arrays.cluster_id, "left"
    ).join(opinions_arrays, opinion_clusters.id == citations_arrays.cluster_id, "left")


def find_latest(directory: str, prefix: str, extension: str) -> str:
    """
    Of the downloads in the directory given the prefix and extension,
    find the one that lexigraphically sorts last. This works because
    the data dumps have iso dates in the middle of them.
    """
    candidates = os.listdir(directory)
    candidates = list(
        filter(lambda x: x.startswith(prefix) and x.endswith(extension), candidates)
    )
    candidates.sort()
    return candidates[-1]


def run(data_dir: str) -> None:
    spark = SparkSession.builder.appName("courtlistener-export").getOrCreate()

    if data_dir.endswith("/"):
        data_dir = data_dir[0:-1]

    print(data_dir)

    latest_citations = data_dir + "/" + find_latest(data_dir, "citations", ".csv.bz2")
    latest_citations_parquet = latest_citations.replace(".csv.bz2", ".parquet")
    parquetify(spark, latest_citations, latest_citations_parquet)

    latest_clusters = data_dir + "/" + find_latest(data_dir, "opinion-clusters", ".bz2")
    latest_clusters_parquet = latest_clusters.replace(".csv.bz2", ".parquet")
    parquetify(spark, latest_clusters, latest_clusters_parquet)

    latest_opinions = data_dir + "/" + find_latest(data_dir, "opinions", ".bz2")
    latest_opinions_parquet = latest_opinions.replace(".csv.bz2", ".parquet")
    parquetify(spark, latest_opinions, latest_opinions_parquet)

    citations = get_citations(spark, latest_citations_parquet)
    opinions = get_opinions(spark, latest_opinions_parquet)
    opinion_clusters = get_opinion_clusters(spark, latest_clusters_parquet)

    reparented = group(citations, opinions, opinion_clusters)
    reparented.write.parquet(data_dir + "/courtlistener.parquet")

    spark.stop()


if __name__ == "__main__":
    run(sys.argv[1])
