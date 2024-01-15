"""
Quick and dirty evaluation of petl's ETL features.
"""
import os
import sys
import petl

# regexes
TRIP_TITLE_PATTERN = r"^(?P<date>\d{1,2}\/\d{1,2}\/\d{4})" \
                     r"[ to]*(?P<end_date>\d{1,2}\/\d{1,2}\/\d{4}){0,1} - " \
                     r"(?P<title>[!&/+,\w\'\"\(\) ]*[a-zA-Z0-9])[ ]?(?P<tags>\[.*\])?$"

TAG_PATTERN = r"\[(?P<distance>[SML])/(?P<difficulty>[EMR])(?P<wet>[W]?)\]"


# constants
NA = ""


# TODO: test the following
# load raw data
# split data into relevant category strings (do regex funcs work?)
# transform relevant fields (e.g. dates)
# save to JSON lines
# read in JSON lines data / bypass processing pipeline

data_path = sys.argv[1]
assert os.path.exists(data_path)

raw_table = petl.fromtext(data_path)

tokenised_table = petl.capture(raw_table,
                               "lines",
                               TRIP_TITLE_PATTERN,
                               ["date", "end_date", "title", "raw_tags"])

# replace None in raw tags with "-" to prevent future regex failure
tokenised_raw_tag_table = petl.convert(tokenised_table, "raw_tags", lambda v: v if v else NA)
print(tokenised_raw_tag_table)

tokenised_tag_table = petl.capture(tokenised_raw_tag_table,
                                   "raw_tags",
                                   TAG_PATTERN,
                                   ["distance", "difficulty", "wet"],
                                   fill=(NA, NA, NA))

print(tokenised_tag_table)
