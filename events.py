"""
Quick and dirty evaluation of petl's ETL features.
"""
import os
import sys
import petl


# TODO: test the following
# load raw data
# split data into relevant category strings (do regex funcs work?)
# transform relevant fields (e.g. dates)
# save to JSON lines
# read in JSON lines data / bypass processing pipeline

data_path = sys.argv[1]
assert os.path.exists(data_path)

raw_table = petl.fromtext(data_path)
print(raw_table)
