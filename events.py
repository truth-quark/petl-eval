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

# petl runs strip() so leading whitespace is cleared
# pattern covers some fiddly cases:
# line 1: optionally find leading hyphen then space (pattern for 2nd event within a date), then a date
# line 2: differentiate between "DATE to DATE" for multi day event
# line 3: handle residual " - " between dates & titles OR the 2nd event in a day pattern
# line 4: extract event title
# line 5: extract optional tag block within "[]" section
EVENT_TITLE_PATTERN = r"^(?:- )?(?P<date>\d{1,2}\/\d{1,2}\/\d{4})?" \
                      r" ?(?:to|-)? (?P<end>\d{1,2}\/\d{1,2}\/\d{4})?" \
                      r"(?:[ ]?- )?" \
                      r"(?P<title>[\w ]*)" \
                      r"(?P<tags>\[[\w,\/\s]+])?"

TAG_PATTERN = r"\[(?P<distance>[SML])/(?P<difficulty>[EMR])(?P<wet>[W]?)\]"


# constants
NA = ""


def event_classifier(title):
    activities = []
    if "MTB" in title:
        activities.append("MTB")
    if "Bushwalk" in title:
        activities.append("Bushwalking")
    return activities if activities else ""


# TODO: test the following
# load raw data
# split data into relevant category strings (do regex funcs work?)
# transform relevant fields (e.g. dates)
# error check date order
# classify event by keyword / add activity compound field
# save to JSON lines
# read in JSON lines data / bypass processing pipeline

data_path = sys.argv[1]
assert os.path.exists(data_path)

raw_table = petl.fromtext(data_path)

tokenised_table = petl.capture(raw_table,
                               "lines",
                               EVENT_TITLE_PATTERN,
                               ["date", "end_date", "title", "raw_tags"])

# replace None in raw tags with NA string to prevent future regex failure
tokenised_raw_tag_table = petl.convert(tokenised_table, "raw_tags", lambda v: v if v else NA)
print(petl.lookall(tokenised_raw_tag_table))

tokenised_tag_table = petl.capture(tokenised_raw_tag_table,
                                   "raw_tags",
                                   TAG_PATTERN,
                                   ["distance", "difficulty", "wet"],
                                   fill=(NA, NA, NA))

tokenised_activity_tag_table = petl.addfield(tokenised_tag_table,
                                             "activities",
                                             lambda rec: event_classifier(rec["title"]))
print(tokenised_activity_tag_table)

# convert dates in functional chained form
date_parser = petl.dateparser("%d/%m/%Y")
processed_table = (petl.convert(tokenised_activity_tag_table, "date", date_parser).convert("end_date", date_parser))
print(processed_table)

print("Exporting tokenised tag table (not date converted)")
petl.tojson(tokenised_activity_tag_table, "/tmp/output.json", lines=True)
