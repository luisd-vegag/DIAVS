import unittest
from AWS_Services.Lambda.data_integration import (
    validate_file_extension,
    validate_file_encoding,
    normalize_headers,
    validate_file_number_of_columns,
    validate_file_columns_names,
    file_content_to_df
)

input_bucket_name = "*-input_raw-zone"
prefix = "Inventory_Per_District/Coahuila/20230115_acereros_inventory.csv"


class TestDataIntegration(unittest.TestCase):

    def test_validate_file_extension(self):
        # Test valid file extension
        file_name = "file.csv"
        expected_extension = "csv"
        self.assertTrue(validate_file_extension(file_name, expected_extension))

        # Test invalid file extension
        file_name = "file.txt"
        expected_extension = "csv"
        self.assertFalse(validate_file_extension(
            file_name, expected_extension))

    def test_validate_file_encoding(self):
        # Test valid file encoding
        file_content = b"\xef\xbb\xbfName,Age\nJohn,30\nJane,25"
        expected_encoding = "utf-8"
        self.assertTrue(validate_file_encoding(
            file_content, expected_encoding))

        # Test invalid file encoding
        file_content = b"Name,Age\nJohn,30\nJane,25"
        expected_encoding = "utf-8"
        self.assertFalse(validate_file_encoding(
            file_content, expected_encoding))

    def test_normalize_headers(self):
        # Test normalizing headers
        file_content = "Name,Age\nJohn,30\nJane,25"
        delimiter = ","
        expected_output = "name,age\nJohn,30\nJane,25"
        self.assertEqual(normalize_headers(
            file_content, delimiter), expected_output)

    def test_validate_file_number_of_columns(self):
        # Test valid number of columns
        file_content = "Name,Age\nJohn,30\nJane,25"
        expected_columns_count = 2
        delimiter = ","
        self.assertTrue(validate_file_number_of_columns(
            file_content, expected_columns_count, delimiter))

        # Test invalid number of columns
        file_content = "Name,Age,Gender\nJohn,30\nJane,25"
        expected_columns_count = 2
        delimiter = ","
        self.assertFalse(validate_file_number_of_columns(
            file_content, expected_columns_count, delimiter))

    def test_validate_file_columns_names(self):
        # Test case where the file has the correct column names
        file_path = "path/to/file.csv"
        expected_columns = ["column1", "column2", "column3"]
        validate_file_columns_names(file_path, expected_columns)
        # Assert that the function did not raise an exception

        # Test case where the file does not have the correct column names
        file_path = "path/to/file2.csv"
        expected_columns = ["column1", "column2", "column3"]
        with self.assertRaises(Exception):
            validate_file_columns_names(file_path, expected_columns)

        # Test case where the file is not a CSV
        file_path = "path/to/file.txt"
        expected_columns = ["column1", "column2", "column3"]
        with self.assertRaises(Exception):
            validate_file_columns_names(file_path, expected_columns)

    def test_file_content_to_df():
        # Test with correct file content and correct column names
        file_content = "name,age,gender\nAlice,25,Female\nBob,32,Male\nCharlie,28,Male"
        column_names = ["name", "age", "gender"]
        expected_df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [
                                   25, 32, 28], "gender": ["Female", "Male", "Male"]})
        assert file_content_to_df(
            file_content, column_names).equals(expected_df)

        # Test with incorrect file content
        file_content = "name,age,gender\nAlice,25,Female\nBob,32\nCharlie,28,Male"
        column_names = ["name", "age", "gender"]
        try:
            file_content_to_df(file_content, column_names)
            assert False, "Expected a ValueError to be raised"
        except ValueError as e:
            assert str(e) == "Incorrect number of columns in file content"

        # Test with incorrect column names
        file_content = "name,age,gender\nAlice,25,Female\nBob,32,Male\nCharlie,28,Male"
        column_names = ["name", "age"]
        try:
            file_content_to_df(file_content, column_names)
            assert False, "Expected a ValueError to be raised"
        except ValueError as e:
            assert str(
                e) == "Number of columns in file content does not match number of column names"
