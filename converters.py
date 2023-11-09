import os.path
import time

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("file_converter")
    .master("local[*]")
    .config("spark.executor.memory", "4G")
    .config("spark.driver.memory", "4G")
    .getOrCreate()
)

start = time.time()


def conversion_timer():
    end = time.time()
    duration = end - start
    print(f"Execution time: {duration} seconds")


def file_converter(
    from_format: str,
    to_format: str,
    original_file: str,
    output_location: str,
    infer_schema=True,
    has_header=True,
) -> None:
    # Read CSV into DataFrame, inferring schema if specified
    df = (
        spark.read.format(from_format.lower())
        .option("inferSchema", infer_schema)
        .option("header", has_header)
        .load(original_file)
    )

    # Convert to path/filename
    filename = original_file.split("/")[-1]
    output_file = output_location + filename

    try:
        # Write DataFrame to Parquet forma
        df.write.format(to_format.lower()).save(output_file)
        print(
            f"Success!\nParquet file {filename} written to {output_file}, size: {os.path.getsize(output_file)} bytes"
        )
        conversion_timer()
    except Exception as e:
        # Print success message
        print(f"Convertion failed!\n{e}")


# def csv_to_parquet(original_file, output_file, use_infer_schema=True, has_header=True):
#     # Read CSV into DataFrame, inferring schema if specified
#     df = spark.read.csv(
#         path=original_file, inferSchema=use_infer_schema, header=has_header
#     )
#
#     # Extract filename from path
#     file_name = original_file.split("/")[-1]
#
#     # Construct output path
#     output = f"{output_file}/{file_name}"
#     file_size = os.path.getsize(output)
#
#     # Write DataFrame to Parquet format
#     df.write.format("parquet").parquet(path=output)
#
#     # Print success message
#     print(f"ParqueArquivo parquet: {output} gerado com sucesso")
# # Converts a CSV file to Avro format
#
#
# # Converts a JSON file to Parquet format
# def json_to_parquet(original_file, output_file):
#     # Read JSON file
#     df = spark.read.json(original_file)
#
#     # Write DataFrame to Parquet
#     df.write.parquet(output_file)
#
#     print(f"Parquet file written to {output_file}")
#
#
# # Converts an ORC file to Avro format
# def orc_to_avro(original_file, output_file, schema):
#     # Read ORC file into DataFrame
#
#     df = spark.read.orc(original_file)
#
#     # Write DataFrame to Avro
#     df.write.format("avro").option("schema", schema).save(output_file)
#
#     print(f"Avro file written to {output_file}")
#
#
# csv_to_parquet(
#     original_file="dataset/NATJUCSV",
#     output_file=f"dataset/parquet",
#     use_infer_schema=True,
# )
#
#
# def csv_to_orc():
#
# def csv_to_rcfile():
#
# def csv_to_sequencefile():
#
# def  json_to_parquet():
#
# def json_to_avro():
#
# def json_to_orc():
#
# def json_to_rcfile():
#
# def json_to_sequencefile():
#
# def parquet_to_csv():
#
# def parquet_to_json():
#
# def parquet_to_avro():
#
# def parquet_to_orc():
#
# def parquet_to_rcfile():
#
# def parquet_to_sequencefile():
#
# def avro_to_csv():
#
# def avro_to_json():
#
# def avro_to_parquet():
#
# def avro_to_orc():
#
# def avro_to_rcfile():
#
# def avro_to_sequencefile():
#
# def orc_to_csv():
#
# def orc_to_json():
#
# def orc_to_parquet():
#
# def orc_to_avro():
#
# def orc_to_rcfile():
#
# def orc_to_sequencefile():
#
# def rcfile_to_csv():
#
# def rcfile_to_json():
#
# def rcfile_to_parquet():
#
# def rcfile_to_avro():
#
# def rcfile_to_orc():
#
# def rcfile_to_sequencefile():
#
# def sequencefile_to_csv():
#
# def sequencefile_to_json():
#
# def sequencefile_to_parquet():
#
# def sequencefile_to_avro():
#
# def sequencefile_to_orc():
#
# def sequencefile_to_rcfile():
