from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("pyspark-notebook")
    .master("local[*]")
    .config("spark.executor.memory", "4G")
    .config("spark.driver.memory", "4G")
    .getOrCreate()
)


def csv_to_parquet(original_file, output_file, use_infer_schema=True, has_header=True):
    # Read CSV into DataFrame, inferring schema if specified
    df = spark.read.csv(
        path=original_file, inferSchema=use_infer_schema, header=has_header
    )

    # Extract filename from path
    filename = original_file.split("/")[-1]

    # Construct output path
    output = f"{output_file}/{filename}"

    # Write DataFrame to Parquet format
    df.write.format("parquet").parquet(path=output)

    # Print success message
    print(f"Arquivo parquet: {output} gerado com sucesso")


# Converts a CSV file to Avro format
def csv_to_avro(original_file, output_file, schema):
    # Read CSV into DataFrame
    df = spark.read.csv(original_file)

    # Write DataFrame to Avro using specified schema
    df.write.format("avro").option("schema", schema).save(output_file)

    print(f"Avro file written to {output_file}")


# Converts a JSON file to Parquet format
def json_to_parquet(original_file, output_file):
    # Read JSON file
    df = spark.read.json(original_file)

    # Write DataFrame to Parquet
    df.write.parquet(output_file)

    print(f"Parquet file written to {output_file}")


# Converts an ORC file to Avro format
def orc_to_avro(original_file, output_file, schema):
    # Read ORC file into DataFrame

    df = spark.read.orc(original_file)

    # Write DataFrame to Avro
    df.write.format("avro").option("schema", schema).save(output_file)

    print(f"Avro file written to {output_file}")


csv_to_parquet(
    original_file="dataset/NATJUCSV",
    output_file=f"dataset/parquet",
    use_infer_schema=True,
)
