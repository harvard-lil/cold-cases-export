# COLD Cases Export

This codebase is used to create a ML-friendly dataset out of the [bulk data downloads](https://www.courtlistener.com/help/api/bulk-data/) available on [courtlistener.com](https://courtlistener.com), a website run by the [Free Law Project](https://free.law/).

## Running:

- Install a relatively new python (Recommended: [pyenv](https://github.com/pyenv/pyenv)).
- Run `pip install pipx`.
- Run `pipx install poetry`.
- In the project directory, run `poetry install`.
- To obtain the latest data, run `poetry run python courtlistener_export/download.py`. This will download the latest versions of each of the bzip2 files needed to compile the dataset to the `data/` subdirectory.
- Install [sdkman](https://sdkman.io/).
- Run `sdk install java 11.0.18-tem`.
- Run `sdk install spark 3.4.0`
- To run the transformation process, run:

```spark-submit --driver-memory 30g --master "local[8]" cold_cases_export/export.py data``` 

Note that the values for the number of cores in the `--master` and `--driver-memory` interact with one another and should be tuned to the system you run it on. These settings worked on a high-end Macbook Pro with an M2 Pro and 32 GB of RAM.

The export script will first convert the data to [Parquet](https://parquet.apache.org/) format if this is the first time processing it. Unfortunately, since the data is bzipped (slow to decompress) and this process cannot be parallelized without some custom low-level code, because the csvs within (particularly the opinions data) have escaped line breaks and the Spark CSV splitter depends on records beginning on individual lines.

The output of the export script will also show up in `data/`. In there, you will find two directories. One called `cold.parquet` which contains the export data in Parquet, and another called `cold.jsonl` which contains the same data in [JSON Lines](https://jsonlines.org/). The data will be split into multiple smaller files, based on the execution plan of the Spark job and the value of `REPARTITION_FACTOR` in the export script.
