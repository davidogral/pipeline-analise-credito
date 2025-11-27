from pathlib import Path
import shutil
from pyspark.sql import SparkSession


def get_spark(app_name: str = "SparkNotebook") -> SparkSession:
    """Create (or reuse) a SparkSession with a consistent configuration."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_single_csv(df, output_file: str, header: bool = True) -> None:
    """Persist a Spark DataFrame as a single CSV file (Spark writes folders by default)."""
    output_path = Path(output_file)
    tmp_dir = output_path.parent / f".{output_path.stem}_spark_tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    df.coalesce(1).write.mode("overwrite").option("header", header).csv(str(tmp_dir))
    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No CSV part files found in {tmp_dir}")
    if output_path.exists():
        output_path.unlink()
    part_files[0].replace(output_path)
    shutil.rmtree(tmp_dir)
