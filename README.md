# BigDataFileConverter
A Python library for easily converting between popular big data file formats. Enables efficient data interchange and transformation for analytics workflows.

## Overview
BigDataFileConverter provides a simple and scalable way to convert files between CSV, JSON, Parquet, Avro, ORC, and other columnar formats. This allows data to be ingested, stored, and queried in the optimal format for different use cases.
The library utilizes Spark DataFrames under the hood, taking advantage of Spark's capabilities for distributed processing of large datasets. Functions are designed for both batch ETL jobs and interactive data exploration.

## Getting Started
Install the package with pip install bigdata-file-converter and import functions as needed. Documentation and examples below demonstrate common usage patterns.

## Key Features
- Intuitive function interfaces for common conversion tasks
- Leverages Spark for scalability and performance on large files
- Support for major columnar formats like Parquet and ORC
- Schema inference and validation where applicable
- Options for compression, encoding, and other I/O settings
- Easy to use from Python, Scala, or as part of ETL workflows
- File Format Support

## Conversion is currently supported between:
- CSV
- JSON
- Parquet

**More formats will be added over time based on demand. Contributions welcome!**

## Usage Examples:
    python:
    # Convert CSV to Parquet for storage
    csv_to_parquet(input_file, output_dir)

    # Convert JSON to Avro with a specified schema
    json_to_avro(data_file, schema, output_path)

    # Convert ORC to Parquet for analysis in Spark
    orc_to_parquet(table, output_path)

See full documentation for all functions and parameters.
