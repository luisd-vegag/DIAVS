from aux_data_integration import *
import unittest
import sys
print("AAAAAAAAAAAAAAAAAAAAAaa")
print(sys.path)
print("AAAAAAAAAAAAAAAAAAAAAaa")

input_bucket_name = "*-input_raw-zone"
prefix = "Inventory_Per_District/Coahuila/20230115_acereros_inventory.csv"


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
        # self.assertFalse(validate_file_extension(file_name, expected_extension))
