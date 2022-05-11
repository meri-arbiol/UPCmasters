import pandas as pd
import numpy as np
import json
import glob
import os

'''
This script concatenates all individual yearly files extracted by `opendatabcn_accidents_collector.py` and produces a unified .json file with all the extracted accidents information, then exports it to the persistent landing zone.
Args:
    (none)
Returns:
    (.json) all_bikez_data.csv - .csv file containing all extracted data from bikez.com
'''

path = r'landing/temporal/opendatabcn-accidents/'
all_files = glob.glob(path + "/*.csv")

li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=0, header=0)
    li.append(df)

df = pd.concat(li, axis=0, ignore_index=True)

df.to_json('landing/persistent/data_solution/accidents_opendata/accidents_opendata.json')