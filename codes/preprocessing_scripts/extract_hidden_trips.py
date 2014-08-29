"""

This script finds the 'hidden trips' in the trip data. 
It looks for bike ids that leave from a station that doesn't
match it's previous destination station. We find the length
of this hidden trip by calculating the lower bound on the time
that it could have left it's previous station, i.e. the time
it arrived from its last trip, and the upper bound, the time 
it leaves its new station. The difference between these two
times is the length of the trip.

Author: Chase Duncan
Contact: cddunca2@illinois.edu
"""

import csv
import calendar
from collections import defaultdict
from tools import *
from datetime import datetime

"""
CONSTANTS
"""
#change this variable to map to input file
DATA_DIR =  "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list_clean.csv"
#change this variable to map to output file
OUTPUT = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/hidden_trips.csv"
DT_FMT = "%Y-%m-%d %H:%M:%S"
NUM_TRIPS = 6402344 
END_OF_TIME = "2014-04-01 00:06:00"
"""
DATA STRUCTURES
"""
logMsg("Initializing data structures.")
#dictionary for mapping to bike_ids to most recently
#known location and the time at which it arrived there
bike_states = defaultdict(lambda : (None, None))

#dictionary that maps station to a set that holds all
#bike_ids that are currently known to be at that station
station_sets = defaultdict(set)

#list for holding all hidden trips
hidden_trips = []

#open the trip file and strip it's header
trips = csv.reader(open(DATA_DIR, "r"))
trips_header = trips.next()

#dictionary for mapping names to ids
names_to_ids = defaultdict()
#we add the null station so that we have a place for trips
#to be born at the beginning of the data set and a place
#for them to go to die at the end. 
names_to_ids['null station'] = '00000'

"""
WORK
"""
process_num = 1
logMsg("Processing trip file.")
for [start_time, end_time, start_station, start_sid, end_station, end_sid,  bike_id] in trips:
	#populate dictionaries so that we have a bijection between station names and station ids
	names_to_ids[start_station] = start_sid
	names_to_ids[end_station] = end_sid

	#this variable is used for outputting updates about where we are at in the data as it 
	#is being processed. 
	process_num += 1
	if(process_num % 10000 == 0):
		logMsg("Still working. Processing trip " + str(process_num) + " of " + str(NUM_TRIPS) + ".")	
	
	#retrieve the station at which we last saw bike and the time it arrived there 
	(curr_station, last_time_seen) = bike_states[bike_id]	

	#update state
	bike_states[bike_id] = (end_station, end_time)
	
	#if the station where this bike_id began this trip
	#does not match its last known state then it is a
	#hidden trip.	
	if(curr_station != start_station and curr_station != None):
		#calculate the length of the hidden trip, i.e. the time difference
		#between when we last saw bike and when it pops up now
		t_1 = datetime.strptime(last_time_seen, DT_FMT)
		t_2 = datetime.strptime(start_time, DT_FMT)
		delta_t = t_2 - t_1
		delta_t = delta_t.total_seconds()
		
		lower_bound_numeric = calendar.timegm(t_1.utctimetuple())
		upper_bound_numeric = calendar.timegm(t_2.utctimetuple())
		
		#these are used in visualization
		lower_date_id = str(t_1.year) + str(t_1.month)
		upper_date_id = str(t_1.year) + str(t_2.month)

		entry = (curr_station, str(names_to_ids[curr_station]), start_station, str(names_to_ids[start_station]), bike_id, last_time_seen, lower_bound_numeric,lower_date_id, start_time, upper_bound_numeric, upper_date_id, delta_t)	
		hidden_trips.append(entry)

#write the hidden trips out to a csv file
output = csv.writer(open(OUTPUT, "w"))
output_header = ["from_station", "from_sid", "to_station", "to_sid", "bike_id", "lower_bound", "lower_bound_numeric","lower_date_id", "upper_bound", "upper_bound_numeric", "upper_date_id", "delta_t"]
output.writerow(output_header)

for hidden_trip in hidden_trips:
	output.writerow(hidden_trip)











