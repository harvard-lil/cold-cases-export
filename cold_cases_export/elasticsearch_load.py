import sys
from pyspark.sql import SparkSession


def run(data_dir: str) -> None:
    if data_dir.endswith("/"):
        data_dir = data_dir[0:-1]

    options = {
        "es.index.auto.create": "true",
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": "cold",
        "es.nodes.wan.only": "true",
        "es.net.ssl": "true",
        "es.net.ssl.cert.allow.self.signed": "true",
        "es.net.http.auth.user": "elastic",
        "es.net.http.auth.pass": "changeme",
    }
    spark = SparkSession.builder.appName("cold-cases-export").getOrCreate()
    spark.read.parquet(data_dir + "/cold.parquet").write.format("es").options(
        **options
    ).save()
    spark.stop()


if __name__ == "__main__":
    run(sys.argv[1])
