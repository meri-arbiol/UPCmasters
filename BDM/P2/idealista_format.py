import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession


def merge_idealista():
    '''
    
    '''

    
def 
                


if __name__ == '__main__':
    '''
    
    '''
    # main code
    directory = "landing/persistent/idealista"
    file_names = []  # List which will store all of the full filepaths.
    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            if filename[-7:] == 'parquet':
                file_names.append(root+'/'+filename)
    
    # spark transformations in sequence for each parquet file
    for file in file_names: