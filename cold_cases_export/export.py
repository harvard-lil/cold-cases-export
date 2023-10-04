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

# will split parquet data this many times for parallelism. may no longer be needed.
OUTPUT_FILES = 32


def get_opinions(dataframe: DataFrame) -> DataFrame:
    """
    Loads opinions from parquet file and cleans up columns
    """
    select_cols = [
        "id",
        "author_str",
        "per_curiam",
        "type",
        "page_count",
        "download_url",
        "plain_text",
        "html",
        "html_lawbox",
        "html_columbia",
        "html_anon_2020",
        "xml_harvard",
        "html_with_citations",
        "extracted_by_ocr",
        "author_id",
        "cluster_id",
    ]
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
        "extracted_by_ocr",
        "cluster_id",
    ]
    drop_cols.extend(text_col_priority)
    return (
        dataframe.alias("o")
        .select(*select_cols)
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
    select_cols = [
        "id",
        "judges",
        "date_filed",
        "date_filed_is_approximate",
        "slug",
        "case_name_short",
        "case_name",
        "case_name_full",
        "attorneys",
        "nature_of_suit",
        "posture",
        "syllabus",
        "headnotes",
        "summary",
        "disposition",
        "history",
        "other_dates",
        "cross_reference",
        "correction",
        "citation_count",
        "precedential_status",
        "blocked",
        "docket_id",
        "arguments",
        "headmatter",
    ]
    drop_cols = ["blocked", "docket_id"]
    return (
        dataframe.alias("oc")
        .select(*select_cols)
        .filter(col("blocked") == "f")
        .withColumn("date_filed", col("date_filed").cast(DateType()))
        .withColumn(
            "date_filed_is_approximate",
            when(col("date_filed_is_approximate") == "t", True).otherwise(False),
        )
        .withColumn("citation_count", col("citation_count").cast(IntegerType()))
        .withColumn("summary", regexp_replace("summary", r"<.+?>", ""))
        .withColumn("id", col("id").cast(IntegerType()))
        .withColumn("opinion_cluster_docket_id", col("docket_id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def get_citations(dataframe: DataFrame) -> DataFrame:
    """
    Loads citations from parquet file and cleans up columns
    """
    select_cols = ["id", "volume", "reporter", "page", "cluster_id"]
    drop_cols = [
        "volume",
        "reporter",
        "page",
        "cluster_id",
    ]
    return (
        dataframe.alias("c")
        .select(*select_cols)
        .withColumn("citation_text", concat_ws(" ", "volume", "reporter", "page"))
        .withColumn("citation_cluster_id", col("cluster_id").cast(IntegerType()))
        .withColumn("id", col("id").cast(IntegerType()))
        .drop(*drop_cols)
    )


def get_dockets(dataframe: DataFrame) -> DataFrame:
    """
    Loads pertinent data from dockets. Only necessary to join court info.
    """
    return (
        dataframe.alias("d")
        .select("id", "court_id")
        .withColumn("id", col("id").cast(IntegerType()))
        .withColumnRenamed("court_id", "docket_court_id")
    )


def get_courts(dataframe: DataFrame) -> DataFrame:
    """
    Loads pertinent data from courts.
    """
    return (
        dataframe.alias("ct")
        .select("id", "short_name", "full_name", "jurisdiction")
        .withColumnRenamed("short_name", "court_short_name")
        .withColumnRenamed("full_name", "court_full_name")
        .withColumnRenamed("jurisdiction", "court_type")
        .withColumn("court_id", col("id"))
        .drop("id")
    )


def get_court_info(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Loads information from a bundled csv containing information about the jurisdiction of courts.
    """
    csv_options = {
        "header": "true",
        "multiLine": "true",
        "quote": '"',
        "escape": '"',
    }

    select_cols = ["court_full_name", "jurisdiction"]
    return (
        spark.read.options(**csv_options)
        .csv(data_dir + "/court-info.csv")
        .select(*select_cols)
        .withColumnRenamed("court_full_name", "court_info_full_name")
        .withColumnRenamed("jurisdiction", "court_jurisdiction")
    )


def group(
    citations: DataFrame,
    opinions: DataFrame,
    opinion_clusters: DataFrame,
    dockets: DataFrame,
    courts: DataFrame,
    court_info: DataFrame,
) -> DataFrame:
    """
    This joins all the dataframes together by their various keys, and removes
    columns we no longer need to see for cleanliness.
    """

    court_and_info = courts.join(
        court_info, court_info.court_info_full_name == courts.court_full_name
    ).drop("court_info_full_name")

    # gets court info ready to join into clusters via docket_id
    dockets_and_courts = dockets.join(
        court_and_info, dockets.docket_court_id == courts.court_id, "left"
    ).withColumnRenamed("id", "dockets_and_courts_id")

    # rolls up citations into arrays to get ready for joining
    citations_arrays = (
        citations.groupby(citations.citation_cluster_id)
        .agg(collect_list(citations.citation_text))
        .alias("citations")
    )

    # columns from opinions we want to keep around post rollup
    opinion_cols = [
        opinions.author_str,
        opinions.per_curiam,
        opinions.type,
        opinions.page_count,
        opinions.download_url,
        opinions.author_id,
        opinions.opinion_text,
        opinions.ocr,
        opinions.opinion_id
    ]

    # rolls up opinions into an array of structs
    opinions_arrays = (
        opinions.groupby(opinions.opinion_cluster_id)
        .agg(collect_list(struct(*opinion_cols)))
        .alias("opinions")
    )

    # joins citation array to clusters
    joined_citations = opinion_clusters.join(
        citations_arrays,
        opinion_clusters.id == citations_arrays.citation_cluster_id,
        "left",
    )

    # renames the citation array column so it looks pretty
    joined_citations = joined_citations.withColumnRenamed(
        joined_citations.columns[-1], "citations"
    )

    # joins opinions arrays in
    joined_opinions = joined_citations.join(
        opinions_arrays,
        opinion_clusters.id == opinions_arrays.opinion_cluster_id,
        "left",
    )

    # renames the opinions array column so it looks pretty
    joined_opinions = joined_opinions.withColumnRenamed(
        joined_opinions.columns[-1], "opinions"
    )

    # joins in courts via dockets
    joined_courts = joined_opinions.join(
        dockets_and_courts,
        dockets_and_courts.dockets_and_courts_id
        == opinion_clusters.opinion_cluster_docket_id,
        "left",
    )

    # cleans up key columns we no longer need to see
    result = joined_courts.drop(
        "opinion_cluster_id",
        "citation_cluster_id",
        "court_id",
        "docket_court_id",
        "dockets_and_courts_id",
        "citation_cluster_id",
        "opinion_cluster_docket_id",
    )

    return result


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
    """
    Converts .csv.bz2 files to parquet for faster processing.
    Unfortunately, since these csvs can have multiline values in them,
    there's not currently a way to parallelize this, since rows can span
    bz2 blocks.
    """
    latest_csv = data_dir + "/" + find_latest(data_dir, nickname, ".csv.bz2")
    latest_parquet = latest_csv.replace(".csv.bz2", ".parquet")
    csv_options = {
        "header": "true",
        "multiLine": "true",
        "quote": '"',
        "escape": '"',
    }
    if not os.path.exists(latest_parquet):
        spark.read.options(**csv_options).csv(latest_csv).write.parquet(latest_parquet)
    return spark.read.parquet(latest_parquet)


def run(data_dir: str) -> None:
    spark = (
        SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "0")
        .appName("cold-cases-export")
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
    court_info = get_court_info(spark, data_dir)

    reparented = group(
        citations, opinions, opinion_clusters, dockets, courts, court_info
    )
    reparented.explain(extended=True)
    reparented.coalesce(OUTPUT_FILES).write.parquet(
        data_dir + "/cold.parquet", compression="gzip"
    )

    spark.read.parquet(data_dir + "/cold.parquet").coalesce(OUTPUT_FILES).write.json(
        data_dir + "/cold.jsonl", compression="gzip"
    )

    spark.stop()


if __name__ == "__main__":
    run(sys.argv[1])
