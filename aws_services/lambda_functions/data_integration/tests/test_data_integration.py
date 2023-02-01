import string
import random
from aux_data_integration import *
import unittest


input_bucket_name = "*-input_raw-zone"
prefix = "Inventory_Per_District/Coahuila/20230115_acereros_inventory.csv"


def generate_random_string(length, encoding):
    letters = string.ascii_letters + string.digits + string.punctuation
    random_str = ''.join(random.choice(letters)
                         for i in range(length))

    random_bytes = random_str.encode(encoding)
    print(f"random_bytes type: {type(random_bytes)}")
    return random_bytes


class TestDataIntegration(unittest.TestCase):

    def test_validate_file_extension(self):
        # Test valid file extension
        file_name = "file.csv"
        expected_extension = "csv"
        self.assertTrue(validate_file_extension(
            file_name, expected_extension))

        # Test invalid file extension
        file_name = "file.txt"
        expected_extension = "csv"
        self.assertFalse(validate_file_extension(
            file_name, expected_extension))
    """
    # This test is commented because i cound not generate a byte object with the desire encoing to test.
    def test_validate_file_encoding(self):
        test_bytes_1 = generate_random_string(100000, "utf-8")
        # Test a file with UTF-8 encoding
        self.assertTrue(validate_file_encoding(
            test_bytes_1, "utf-8"))

        # Test a file with ISO-8859-1 encoding
        test_bytes_2 = generate_random_string(100000, "iso-8859-1")
        self.assertTrue(validate_file_encoding(
            test_bytes_2, "iso-8859-1"))

        # Test a file with a different encoding than expected
        test_bytes_3 = generate_random_string(100000, "utf-8")
        self.assertTrue(validate_file_encoding(
            test_bytes_3, "iso-8859-1")) 
    """

    def test_normalize_headers(self):
        # Test a file with headers containing special characters
        file_content = "Column 1, Column 2 (with special chars), Column-3\nValue 1, Value2, Value-3\nvalue4,value5,value6"
        delimiter = ","
        expected_output = "column_1,column_2_with_special_chars,column_3\nValue 1, Value2, Value-3\nvalue4,value5,value6"
        self.assertEqual(normalize_headers(
            file_content, delimiter), expected_output)

        # Test a file with headers containing only lowercase letters
        file_content = "column 1| column 2| column 3|\nValue 1, Value2, Value-3\nvalue4,value5,value6"
        delimiter = "|"
        expected_output = "column_1|column_2|column_3|\nValue 1, Value2, Value-3\nvalue4,value5,value6"
        self.assertEqual(normalize_headers(
            file_content, delimiter), expected_output)

    def test_validate_file_number_of_columns(self):
        file_content = "column1,column2,column3\nvalue1,value2,value3\nvalue4,value5,value6"
        expected_columns_count = 3
        delimiter = ","

        result = validate_file_number_of_columns(
            file_content, expected_columns_count, delimiter)
        self.assertTrue(result)

        file_content = "column1|column2|column3\nvalue1|value2\nvalue4|value5|value6"
        expected_columns_count = 2
        delimiter = "|"
        result = validate_file_number_of_columns(
            file_content, expected_columns_count, delimiter)
        self.assertFalse(result)

    def test_validate_file_columns_names(self):
        file_content = "header1,header2,header3\nvalue1,value2,value3"
        columns_details = [{"header": "header1"},
                           {"header": "header2"},
                           {"header": "header3"}]
        delimiter = ","

        result = validate_file_columns_names(
            file_content, columns_details, delimiter)
        self.assertTrue(result)

        file_content = "header1,header2,header3\nvalue1,value2,value3"
        columns_details = [{"header": "header1"},
                           {"header": "header2"},
                           {"header": "header_3"}]
        delimiter = ","

        result = validate_file_columns_names(
            file_content, columns_details, delimiter)
        self.assertFalse(result)

    def test_add_date_columns(self):
        # Test 1: check adding parameter_date and file_date columns
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6], "parameter_date_col": [
                          "2021-01-01", "2021-01-02", "2021-01-03"]})
        date_details = {
            "parameter_date": "parameter_date_col", "file_date": True}
        file_name = "test_file.csv"
        output_base_file_name = "test_file"
        df, output_file_name = add_date_columns(
            df, date_details, file_name, output_base_file_name)
        self.assertTrue("parameter_date" in df.columns)
        self.assertTrue("file_date" in df.columns)
        self.assertEqual(df["parameter_date"].dtype, "datetime64[ns]")
        self.assertEqual(df["file_date"].dtype, "datetime64[ns]")
        self.assertTrue(
            re.match(r"test_file_\d{8}$", output_file_name))

        # Test 2: check adding source_date and file_date columns
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        date_details = {"source_date": {"date_regex": r"\d{8}",
                                        "date_format": "%Y%m%d"}, "file_date": True}
        file_name = "20210101_test_file.csv"
        output_base_file_name = "test_file"
        df, output_file_name = add_date_columns(
            df, date_details, file_name, output_base_file_name)
        self.assertTrue("source_date" in df.columns)
        self.assertTrue("file_date" in df.columns)
        self.assertEqual(df["source_date"].dtype, "datetime64[ns]")
        self.assertEqual(df["file_date"].dtype, "datetime64[ns]")
        self.assertTrue(
            re.match(r"^20210101_test_file_\d{8}$", output_file_name))

        # Test 3: check adding only source_date columns
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        date_details = {"source_date": {
            "date_regex": r"\d{8}", "date_format": "%Y%m%d"}}
        file_name = "20210101_test_file.csv"
        output_base_file_name = "test_file"
        df, output_file_name = add_date_columns(
            df, date_details, file_name, output_base_file_name)
        self.assertTrue("source_date" in df.columns)
        self.assertEqual(df.iloc[0]["source_date"],
                         pd.to_datetime("2021-01-01"))
        self.assertEqual(df.iloc[1]["source_date"],
                         pd.to_datetime("2021-01-01"))
        self.assertEqual(df.iloc[2]["source_date"],
                         pd.to_datetime("2021-01-01"))
        self.assertEqual(output_file_name, "20210101_test_file")

        # Test 4: check adding all columns (parameter_date, source_date, file_date)
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        date_details = {
            "parameter_date": "col1",
            "source_date": {"date_regex": r"\d{8}", "date_format": "%Y%m%d"},
            "file_date": True
        }
        file_name = "20210101_test_file.csv"
        output_base_file_name = "test_file"
        now = datetime.now().strftime("%Y%m%d")
        df, output_file_name = add_date_columns(
            df, date_details, file_name, output_base_file_name)
        self.assertTrue("parameter_date" in df.columns)
        self.assertTrue("source_date" in df.columns)
        self.assertTrue("file_date" in df.columns)
        self.assertEqual(len(df.columns), 5)
        self.assertEqual(
            output_file_name, f"20210101_test_file_{now}")

    def test_get_record_delimiter(self):
        file_extension = 'csv'
        encoding = 'utf-8'
        result = get_record_delimiter(file_extension, encoding)
        self.assertEqual(result, '\n')
