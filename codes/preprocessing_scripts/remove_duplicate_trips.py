"""
Removes the duplicate trips from the master_trip_list.csv.
From whence did they come? We do not know.

Author: Chase Duncan
"""
import csv
from tools import *

"""
CONSTANTS
"""
DATA_DIR = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list.csv"

"""
DATA STRUCTURES
"""
output_list = []

logMsg("Reading in trip file.")
input_file = csv.reader(open(DATA_DIR, "r"))
dup_file = csv.writer(open("/Users/chaseduncan/Desktop/capital/data/capital_trip_data/duplicate_trips.csv", "w"))
#first 'previous' trip is the header
[prev_dur, prev_st, prev_et, prev_start, prev_start_sid, prev_end, prev_end_sid, prev_bike, prev_type] = input_file.next()

#keeps track of how many duplicates we find. just for fun!
dup_count = 0
prev_trip_all = None

#used for outputting updates
trip_count = 1
logMsg("Looking for duplicate trips.")
#iterate over trip file and check to see if the current trip is the same as the previous trip. 
#if it is up increment our dup counter and the continue without adding the trip to the out_list.
for [duration,start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id,mem_type] in input_file:
	if(trip_count % 50000 == 0):
		logMsg("Still working. Processing trip " + str(trip_count) + " of 6402343.")
	trip_count += 1

	if(prev_st == start_time and prev_et == end_time and prev_start_sid == start_sid and prev_end_sid == end_sid and prev_bike == bike_id):
		#write out the duplicates to see if they're reasonable
		dup_file.writerow(prev_trip_all)
		dup_file.writerow([duration,start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id,mem_type])	

		dup_count += 1
		continue

	else:
		entry = (start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id)
		output_list.append(entry)

	#current trip is now previous trip, obv
	prev_st = start_time
	prev_et = end_time
	prev_start_sid = start_sid
	prev_end_sid = end_sid
	prev_bike = bike_id
	prev_trip_all = [duration,start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id,mem_type]

logMsg(str(dup_count) + " duplicate trips found.")
logMsg("Writing output.")

output_file = csv.writer(open("/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list_clean.csv", "w"))
header = ["start_time", "end_time", "start_station", "start_sid", "end_station", "end_sid", "bike_id"]
output_file.writerow(header)

for trip in output_list:
	output_file.writerow(trip)

logMsg("Done.")







