from veetility.utility_functions import UtilityFunctions
import unittest
import os
import pandas as pd

class TestWriteJson(unittest.TestCase):

    def setUp(self):
        self.test_folder = "test_JSON_files"
        self.df = pd.DataFrame({
            'col1' : [1, 2, 3],
            'col2' : [4, 5, 6]
        })
        self.file_name = "test_dataframe"
        self.util_class = UtilityFunctions()
    
    def test_df_to_json(self):
        """Test writing a DataFrame to a JSON file"""
        self.util_class.write_json(self.df, 
                                   "test_dataframe", 
                                   'DataFrame',
                                   self.test_folder)
        
        self.assertTrue(os.path.isfile(f"{self.test_folder}/test_dataframe.json"))

        with open(f"{self.test_folder}/test_dataframe.json", 'r') as f:
            data = json.load(f)
        
        self.a
        
