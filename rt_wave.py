'''
Problems:
1) First time run isn't great, probably dies on missing files
2) Reading all of the ids into memory is going to become a burden fast
3) Related to 2: probably will want to break up the files, perhaps into days
'''

import urllib
import re
import hashlib
import datetime
import csv
import os
import sys

BUOYS = [
'46012',
'46237',
'46214'
]

RTWAVE = os.path.abspath("./rt_wave")
if not os.path.isdir(RTWAVE):
    os.mkdir(RTWAVE)


'''
Each line contains 15 columns:
1: Year
2: Month
3: Day
4: Hour
5: Minute
6: Wave height
7: 
'''

DATA_MAP = ['buoy', 'id', 'WVHT', 'SwH', 'SwP', 'WWH', 'WWP', 'SwD', 'WWD', 'STEEPNESS', 'APD', 'MWD']

def is_id_in_file(id, buoy_dir, fname):
    abspath = '/'.join([buoy_dir, fname])
    if os.path.isfile(abspath):
        handle = open(abspath)
        txt = handle.read()
        handle.close()
        return id in txt
    return False

def write_splunk_event(buoy_dir, fname, timestamp, data):
    f = open('/'.join([buoy_dir, fname]), 'a')
    outstr = timestamp + " " + ' '.join(["%s=%s" % (item, data[item]) for item in data]) + "\n"
    f.write(outstr)
    f.close()
    return True

def process_buoy(buoy):
    handle = urllib.urlopen('http://www.ndbc.noaa.gov/data/realtime2/%s.spec' % buoy)
    lines = handle.readlines()

    # Build the buoy's dir
    buoy_dir = "%s/%s" % (RTWAVE, buoy)
    if not os.path.isdir(buoy_dir):
        os.mkdir(buoy_dir)
    
    # Reverse the order of the lines and exclude the now last two
    for line in lines[:1:-1]:

        # Generate a new id for each event
        id = hashlib.md5(line).hexdigest()

        # Split on whitespaces
        parts = re.split('\s+', line.strip())

        # Build a list of date parts
        date_parts = [int(dpart) for dpart in parts[0:5]]

        # Build date and datetimes
        fdate = datetime.date(*date_parts[:3])

        if is_id_in_file(id, buoy_dir, fdate.isoformat()):
            continue

        fields = [buoy, id]
        fields.extend(parts[5:])
        out = {}
        for i, k in enumerate(DATA_MAP):
            out[k] = fields[i]

        fdatetime = datetime.datetime(*date_parts)
        timestamp = fdatetime.strftime("%Y-%m-%d %H:%M:%S")
        write_splunk_event(buoy_dir, fdate.isoformat(), timestamp, out)

if __name__ == '__main__':
    for buoy in BUOYS:
        process_buoy(buoy)




















