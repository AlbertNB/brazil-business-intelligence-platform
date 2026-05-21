import argparse
import re
from typing import List, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze loader for JSON sources using Databricks Auto Loader (Unity Catalog–safe).")

    # What to load
    p.add_argument("--source", required=True, help="Source name (e.g. ibge, rfb).")
    p.add_argument("--path_domain", default="", help="Optional domain layer in path (e.g. cnpj/).")
    p.add_argument("--streams", default="*", help="Comma-separated list or '*' to discover from landing.")
    p.add_argument("--format", default="json", help="Auto Loader format (json, csv).")
    p.add_argument("--encoding", default="UTF-8", help="Input file encoding. Default=UTF-8.")
    p.add_argument(
        "--multiline",
        default="true",
        help="Multiline records flag (true/false). Useful for JSON arrays and CSV fields containing line breaks.",
    )

    # Optional CSV-specific behavior
    p.add_argument(
        "--header",
        default="true",
        help="CSV header flag (true/false). Default=true. Use false for files without column names."
    )
    p.add_argument("--delimiter", default=",", help="CSV delimiter. Default=','")
    p.add_argument("--quote", default='"', help="CSV quote character. Default='\"'")
    p.add_argument("--escape", default='"', help="CSV escape character. Default='\"'")

    # Optional partitioning behavior
    p.add_argument(
        "--partition_by_column",
        default="",
        help="Optional Bronze partition column (e.g. reference_month). If empty, no partitioning is applied."
    )

    # Storage roots (required)
    p.add_argument("--landing_root", required=True, help="Landing root path (e.g. s3://bbip-landing-prod-<account>-<region>/).")
    p.add_argument("--autoloader_root", required=True, help="Auto Loader root for schema/checkpoints (e.g. s3://bbip-metadata-prod-<account>-<region>/autoloader).")
    p.add_argument("--bronze_root", required=True, help="Bronze root path (e.g. s3://bbip-bronze-prod-<account>-<region>/).")

    # Unity Catalog
    p.add_argument("--catalog", default="bbip_prod", help="Unity Catalog catalog name.")
    p.add_argument("--target_schema", default="bronze", help="Target schema (database) name inside catalog.")

    return p.parse_args()


def sql_safe(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def join_path(*parts: str) -> str:
    return "/".join([p.strip("/").replace("\\", "/") for p in parts if p])


def build_paths(args: argparse.Namespace, stream: str) -> Dict[str, str]:
    base = join_path(args.landing_root, args.source, args.path_domain) if args.path_domain else join_path(args.landing_root, args.source)
    src = join_path(base, stream) + "/"

    table_name = f"{sql_safe(args.source)}__{sql_safe(stream)}"

    table = f"{args.catalog}.{args.target_schema}.{table_name}"

    out = join_path(args.bronze_root, sql_safe(args.source), table_name) + "/"
    sch = join_path(args.autoloader_root, sql_safe(args.source), table_name, "schema") + "/"
    ckpt = join_path(args.autoloader_root, sql_safe(args.source), table_name, "checkpoint") + "/"

    return {
        "src": src,
        "schema_loc": sch,
        "checkpoint": ckpt,
        "out": out,
        "table": table,
        "landing_base": base + "/"
    }


def list_streams_from_landing(dbutils, landing_base: str) -> List[str]:
    items = dbutils.fs.ls(landing_base)
    streams = sorted([it.name.rstrip("/") for it in items if it.isDir()])
    return streams


def build_reader(spark: SparkSession, args: argparse.Namespace, paths: Dict[str, str]):
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", args.format)
        .option("cloudFiles.schemaLocation", paths["schema_loc"])
        .option("encoding", args.encoding)
    )

    if args.format.lower() == "json":
        reader = reader.option("multiLine", args.multiline.lower())

    if args.format.lower() == "csv":
        reader = (
            reader
            .option("header", str(args.header).lower())
            .option("multiLine", args.multiline.lower())
            .option("delimiter", args.delimiter)
            .option("quote", args.quote)
            .option("escape", args.escape)
        )

    return reader


def enrich_partition_column(df, args: argparse.Namespace):
    partition_col = (args.partition_by_column or "").strip()

    if not partition_col:
        return df

    if partition_col in df.columns:
        return df

    # Dynamically derive from file path in the format .../<partition_col>=<value>/...
    pattern = rf"(?:^|/){re.escape(partition_col)}=([^/]+)"
    return df.withColumn(
        partition_col,
        F.regexp_extract(F.col("_metadata.file_path"), pattern, 1)
    )


def run_one_stream(spark: SparkSession, args: argparse.Namespace, stream: str, paths: Dict[str, str]) -> Dict[str, Any]:
    reader = build_reader(spark, args, paths)

    df = (
        reader.load(paths["src"])
              .withColumn("_ingestion_ts", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    df = enrich_partition_column(df, args)

    df = df.withColumn("_raw", F.to_json(F.struct("*")))

    partition_col = (args.partition_by_column or "").strip()

    writer = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", paths["checkpoint"])
          .option("path", paths["out"])
          .outputMode("append")
          .trigger(availableNow=True)
    )

    if partition_col:
        writer = writer.partitionBy(partition_col)

    q = writer.start()
    q.awaitTermination()

    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {paths["table"]}
      USING DELTA
      LOCATION '{paths["out"]}'
    """)

    return {
        "source": args.source,
        "stream": stream,
        "table": paths["table"],
        "landing": paths["src"],
        "bronze_path": paths["out"],
        "checkpoint": paths["checkpoint"],
        "schema_location": paths["schema_loc"],
        "partition_by_column": partition_col,
    }


def main():
    args = parse_args()

    spark = SparkSession.builder.getOrCreate()

    try:
        dbutils  # type: ignore  # noqa
    except NameError:
        raise RuntimeError("This script must run on Databricks (dbutils is required).")

    spark.sql(f"USE CATALOG {args.catalog}")

    sample_paths = build_paths(args, stream="__dummy__")
    landing_base = sample_paths["landing_base"]

    if args.streams.strip() in ("", "*"):
        streams = list_streams_from_landing(dbutils, landing_base)
    else:
        streams = [s.strip() for s in args.streams.split(",") if s.strip()]

    if not streams:
        raise RuntimeError(f"No streams found/selected under: {landing_base}")

    results, errors = [], []

    for stream in streams:
        try:
            paths = build_paths(args, stream)
            results.append(run_one_stream(spark, args, stream, paths))
        except Exception as e:
            errors.append({"stream": stream, "error": str(e)})

    print("RESULTS:")
    for r in results:
        print(r)

    if errors:
        print("ERRORS:")
        for er in errors:
            print(er)
        raise RuntimeError(f"Failures in {len(errors)} stream(s). See driver logs.")


if __name__ == "__main__":
    main()
