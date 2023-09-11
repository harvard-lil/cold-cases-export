import os
import sys

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

REPARTITION_FACTOR = 32  # will split data this many times for parallelism


def get_opinions(dataframe: DataFrame) -> DataFrame:
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
        "cluster_id",
    ]
    drop_cols.extend(text_col_priority)
    return (
        dataframe.alias("o")
        .withColumn("opinion_text", coalesce(*text_col_priority))
        .withColumn("opinion_text", regexp_replace("opinion_text", r"<.+?>", ""))
        .withColumn("page_count", col("page_count").cast(IntegerType()))
        .withColumn("ocr", when(col("extracted_by_ocr") == "t", True).otherwise(False))
        .withColumn("per_curiam", when(col("per_curiam") == "t", True).otherwise(False))
        .withColumn("opinion_cluster_id", col("cluster_id").cast(IntegerType()))
        .withColumn("author_id", col("author_id").cast(IntegerType()))
        .withColumn("opinion_id", col("id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def get_opinion_clusters(dataframe: DataFrame) -> DataFrame:
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
        "blocked",
    ]
    return (
        dataframe.alias("oc")
        .withColumn("date_filed", col("date_filed").cast(DateType()))
        .withColumn(
            "date_filed_is_approximate",
            when(col("date_filed_is_approximate") == "t", True).otherwise(False),
        )
        .withColumn("date_blocked", col("date_blocked").cast(DateType()))
        .withColumn("citation_count", col("citation_count").cast(IntegerType()))
        .withColumn("summary", regexp_replace("summary", r"<.+?>", ""))
        .withColumn("id", col("id").cast(IntegerType()))
        .withColumn("docket_id", col("docket_id").cast(IntegerType()))
        .filter(col("blocked") == "f")
        .drop(*drop_cols)
    )


def get_citations(dataframe: DataFrame) -> DataFrame:
    """
    Loads citations from parquet file and cleans up columns
    """
    drop_cols = [
        "volume",
        "reporter",
        "page",
        "cluster_id",
        "type",  # might be needed. no current lookup table provided.
    ]
    return (
        dataframe.alias("c")
        .withColumn("citation_text", concat_ws(" ", "volume", "reporter", "page"))
        .withColumn("citation_cluster_id", col("cluster_id").cast(IntegerType()))
        .withColumn("id", col("id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def get_dockets(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.alias("d")
        .select("id", "case_name_sort", "case_name_full", "slug", "court_id")
        .withColumn()
    )


def get_courts(dataframe: DataFrame) -> DataFrame:
    #  Todo
    return dataframe


def group(
    citations: DataFrame,
    opinions: DataFrame,
    opinion_clusters: DataFrame,
    dockets: DataFrame,
    courts: DataFrame,
) -> DataFrame:
    """
    This groups the three datasets by cluster id and then merges them using
    reparent_opinions. There's a better way to do this in the pure dataframe
    api, but I didn't get around to figuring it out.
    """

    dockets_and_courts = dockets.join(courts, dockets.court_id == courts.id, "left")

    citations_arrays = citations.groupby(citations.citation_cluster_id).agg(
        collect_list(struct(*[col(c).alias(c) for c in citations.columns]))
    )

    opinions_arrays = opinions.groupby(opinions.opinion_cluster_id).agg(
        collect_list(struct(*[col(c).alias(c) for c in opinions.columns]))
    )

    return (
        opinion_clusters.join(
            citations_arrays,
            opinion_clusters.id == citations_arrays.citation_cluster_id,
            "left",
        )
        .join(
            opinions_arrays,
            opinion_clusters.id == opinions_arrays.opinion_cluster_id,
            "left",
        )
        .join(
            dockets_and_courts,
            dockets_and_courts.id == opinion_clusters.docket_id,
            "left",
        )
    )


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


def parquetify(spark: SparkSession, data_dir: str, nickname: str) -> DataFrame:
    latest_csv = data_dir + "/" + find_latest(data_dir, nickname, ".csv.bz2")
    latest_parquet = latest_csv.replace(".csv.bz2", ".parquet")
    csv_options = {
        "header": "true",
        "multiLine": "true",
        "quote": '"',
        "escape": '"',
    }
    if not os.path.exists(latest_parquet):
        spark.read.options(**csv_options).csv(latest_csv).repartition(
            REPARTITION_FACTOR
        ).write.parquet(latest_parquet)
    return spark.read.parquet(latest_parquet)


def run(data_dir: str) -> None:
    spark = (
        SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "0")
        .appName("courtlistener-export")
        .getOrCreate()
    )

    if data_dir.endswith("/"):
        data_dir = data_dir[0:-1]

    citations = get_citations(parquetify(spark, data_dir, "citations"))
    opinions = get_opinions(parquetify(spark, data_dir, "opinions"))
    opinion_clusters = get_opinion_clusters(
        parquetify(spark, data_dir, "opinion-clusters")
    )
    courts = get_courts(parquetify(spark, data_dir, "courts"))
    dockets = get_dockets(parquetify(spark, data_dir, "dockets"))

    reparented = group(citations, opinions, opinion_clusters, dockets, courts)
    reparented.explain(extended=True)
    reparented.write.parquet(data_dir + "/courtlistener.parquet")

    spark.read.parquet(data_dir + "/courtlistener.parquet").write.json(
        data_dir + "/courtlistener.jsonl", compression="gzip"
    )

    spark.stop()


if __name__ == "__main__":
    run(sys.argv[1])
