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
EVENT_ACTIVITY_KEYWORDS = ("MTB", "Bushwalk")


def event_classifier(title):
    return [kw for kw in EVENT_ACTIVITY_KEYWORDS if kw.lower() in title.lower()] or None


# TODO: implement the following steps to assess petl library (ignoring architecture)
# DONE load raw data
# split data into relevant category strings (do regex funcs work?)
# DONE transform relevant fields (e.g. dates)
# DONE classify event by keyword / add activity compound field
# error check date order
# PART save to JSON lines
# read in JSON lines (data index) / bypass processing pipeline

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

# perform date ETL operations in chained/functional form
# fill missing dates where multiple events fall on the same day
# add date validation fields

def check_prev_date(previous, current, _):
    """True if current event starts after the last event (may occur on same day)"""
    if previous is None:
        return None

    if previous.end_date:
        # verify current event doesn't start before or during previous event
        return current.date > previous.date and current.date >= previous.end_date

    return current.date >= previous.date


date_parser = petl.dateparser("%d/%m/%Y")
processed_table = (petl
                   .convert(tokenised_activity_tag_table, "date", date_parser)
                   .convert("end_date", date_parser)
                   .filldown("date")
                   .addfieldusingcontext("starts_after_last", check_prev_date))

print("Full form data")
print(petl.lookall(processed_table))
print("\nShort form data")
print(processed_table)

print("Exporting tokenised tag table (not date converted)")
petl.tojson(tokenised_activity_tag_table, "/tmp/output.json", lines=True)
