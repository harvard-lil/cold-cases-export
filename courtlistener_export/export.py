import os
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import IntegerType, DateType, ArrayType, StringType
from pyspark.sql.functions import (
    coalesce,
    when,
    col,
    regexp_replace,
    concat_ws,
)


def parquetify(in_path: str, out_path: str) -> None:
    """
    Conditionally converts a .csv.bz2 to parquet for faster processing and filtering.
    :param in_path: path to .csv.bz2
    :param out_path: name of output file
    :return: None
    """
    csv_options = {
        "header": "true",
        "multiLine": "true",
        "quote": '"',
        "escape": '"',
    }
    if not os.path.exists(out_path):
        spark.read.options(**csv_options).csv(in_path).write.parquet(out_path)


def get_opinions(path: str) -> DataFrame:
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


def get_opinion_clusters(path: str) -> DataFrame:
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


def get_citations(path: str) -> DataFrame:
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
    citations: DataFrame, opinions: DataFrame, opinion_clusters: DataFrame
) -> DataFrame:
    """
    This groups the three datasets by cluster id and then merges them using
    reparent_opinions. There's a better way to do this in the pure dataframe
    api, but I didn't get around to figuring it out.
    """

    def reparent_opinions(row: Row) -> Row:
        pair = row[1]
        cluster = list(list(pair[0])[0][0])[0]
        opinions = list(list(pair[0])[0][1])
        citations = list(pair[1])
        dict = cluster.asDict()
        dict["opinions"] = opinions
        dict["citations"] = citations
        return Row(**dict)

    citations_keyed = citations.rdd.map(lambda x: (x.cluster_id, x.citation_text))
    opinions_keyed = opinions.rdd.map(lambda x: (x.cluster_id, x))

    grouped = (
        opinion_clusters.rdd.map(lambda x: (x.id, x))
        .groupWith(opinions_keyed)
        .groupWith(citations_keyed)
    )

    schema = opinion_clusters.schema
    schema.add("opinions", ArrayType(opinions.schema))
    schema.add("citations", ArrayType(StringType()))

    return (
        grouped.map(lambda x: reparent_opinions(x))
        .toDF(schema)
        .drop("opinions.cluster_id")
    )


def find_latest(directory: str, prefix: str, extension: str) -> str:
    candidates = os.listdir(directory)
    candidates = list(
        filter(lambda x: x.startswith(prefix) and x.endswith(extension), candidates)
    )
    candidates.sort()
    return candidates[-1]


if __name__ == "__main__":
    data_dir = "data/"

    spark = (
        SparkSession.builder.appName("courtlistener-export")
        .master("local[8]")  # todo
        .config("spark.driver.memory", "28g")  # todo
        .getOrCreate()
    )

    latest_citations = data_dir + find_latest(data_dir, "citations", ".csv.bz2")
    latest_citations_parquet = latest_citations.replace(".csv.bz2", ".parquet")
    parquetify(latest_citations, latest_citations_parquet)

    latest_clusters = data_dir + find_latest(data_dir, "opinion-clusters", ".bz2")
    latest_clusters_parquet = latest_clusters.replace(".csv.bz2", ".parquet")
    parquetify(latest_clusters, latest_clusters_parquet)

    latest_opinions = data_dir + find_latest(data_dir, "opinions", ".bz2")
    latest_opinions_parquet = latest_opinions.replace(".csv.bz2", ".parquet")
    parquetify(latest_opinions, latest_opinions_parquet)

    citations = get_citations(latest_citations_parquet)
    opinions = get_opinions(latest_opinions_parquet)
    opinion_clusters = get_opinion_clusters(latest_clusters_parquet)

    reparented = group(citations, opinions, opinion_clusters)
    reparented.write.parquet(data_dir + "courtlistener.parquet")
