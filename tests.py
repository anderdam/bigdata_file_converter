import os
import unittest

from converters import file_converter


class TestCases(unittest.TestCase):
    def test_csv_to_parquet(self):
        file_converter("csv", "parquet", "fileTests/NATJUCSV", "fileTests/parquet/")
        self.assertTrue(os.path.exists("fileTests/parquet/NATJUCSV"))

    def test_parquet_to_json(self):
        file_converter(
            "PARQUET",
            "json",
            "fileTests/parquet/NATJUCSV",
            "fileTests/json/",
        )
        self.assertTrue(os.path.exists("fileTests/json/NATJUCSV"))

    def test_json_to_csv(self):
        file_converter("json", "csv", "fileTests/json/NATJUCSV", "fileTests/csv/")
        self.assertTrue(os.path.exists("fileTests/csv/NATJUCSV"))


if __name__ == "__main__":
    test_order = [
        "test_csv_to_parquet",
        "test_parquet_to_avro",
        "test_avro_to_sequencefile",
    ]
    test_loader = unittest.TestLoader()
    test_loader.sortTestMethodsUsing = lambda x, y: test_order.index(
        x
    ) - test_order.index(y)
    unittest.main(testLoader=test_loader)
