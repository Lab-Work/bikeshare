"""
Parses the trip data. For each station it 
tracks in trips and out trips as +/- 1, respectively.
The output is a file per station that tracks the
relative capacity beginning at zero and associates it
with a UTC timestamp so it can be easily graphed in R.

Author: Chase Duncan
"""

import csv
import calendar
from datetime import datetime
from collections import defaultdict
from tools import *

DATA_DIR = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list_clean.csv"
OUT_DIR = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/relative_station_occupancies/"
FMT = "%Y-%m-%d %H:%M:%S"


#for each station we maintain a list of events.
#an event is a tuple that is a timestamp and
#and +/-1.
events_by_station = defaultdict(list)

#dictionary for mapping ids to station names
ids_to_names = defaultdict()
logMsg("Reading in data.")
input_file = csv.reader(open(DATA_DIR, "r"))
input_file.next()

logMsg("Parsing data.")
#iterate like you just don't care
for [start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id] in input_file:
	ids_to_names[start_sid] = start_station
	ids_to_names[end_sid] = end_station

	start_t = datetime.strptime(start_time, FMT)
	end_t = datetime.strptime(end_time, FMT)

	start_stamp = calendar.timegm(start_t.utctimetuple())
	end_stamp = calendar.timegm(end_t.utctimetuple())
	
	in_trip = (end_stamp, 1)
	out_trip = (start_stamp, -1)
	
	events_by_station[start_sid].append(out_trip)
	events_by_station[end_sid].append(in_trip)

for station in events_by_station:
	logMsg("Writing out " + ids_to_names[station] + " station data.")
	#get output file together
	output_dir = OUT_DIR + station + ".csv"
	out_file = csv.writer(open(output_dir, "w")) 
	out_file.writerow(["timestamp", "date", "date_id", "occupancy" ])
	
	#used to track occupancy of current station
	occupancy = 0
	#sort the events by time
	events_by_station[station].sort()
	
	#do it!
	for event in events_by_station[station]:
		occupancy += event[1]
		date = datetime.fromtimestamp(event[0])
		date_id = str(date.year) + str(date.month) 
		out_file.writerow([event[0], date, date_id, occupancy])	

logMsg("Done.")






