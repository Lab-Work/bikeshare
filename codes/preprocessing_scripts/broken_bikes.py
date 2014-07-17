"""
An attempt to find broken bikes
in the Capital Bikeshare system.

Author: Chase Duncan
"""

import csv
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from tools import *

DATA_IN = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list.csv"
DATA_OUT = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/bikes_last_seen.csv"
PROB_OUT =  "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_probs.csv"

LAST_DATE = "2014-04-01 00:06:00"
FMT = "%Y-%m-%d %H:%M:%S"
total_trips = 6402343

PRIOR_BROKEN = .01 

class Bike:
	def __init__(self, bike_id):
		self.time_delta = None
		self.id = bike_id
		self.last_station = None
		self.last_sid = None
		self.date_arrived = None
		self.ins = 0
		self.outs = 0

#Dictionary that maps bike_id to the last station at which it was seen.
last_seen = defaultdict(lambda: None)

#Dictionary of dictionary of states of each station. Each station will have 
#a list of tuples of the form (bike_id, station, date_arrived, ins, outs) which
#keeps track of each bike that is at the station, the date it arrived
#and how many other bikes have come in or out since it arrived.
stations = defaultdict(dict)
trip_list = []
station_error = set()
#dictionary for looking up capacities of stations
capacities = defaultdict()
station_states = csv.reader(open("/Users/chaseduncan/Desktop/capital/data/stations_info.csv", "r"))
ss_header = station_states.next()
for [station_id, station_name, terminal_name, install_date, latitude, longitude, capacity] in station_states:
	capacities[terminal_name] = int(capacity)
	
#Read in the csv and strip off the header.
in_file = csv.reader(open(DATA_IN, "r"))
header = in_file.next()

#Variable used for updating output messages.
iter_count = 1

station_names_not_found = open("/Users/chaseduncan/Desktop/capital/data/station_names_not_found.txt", "a")

for [duration, start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id, mem_type] in in_file:
	new_entry = Bike(bike_id)
	new_entry.last_station = end_station
	new_entry.last_sid = end_sid
	new_entry.date_arrived = end_time

	if(iter_count % 10000 == 0):
		logMsg("Processing trip " + str(iter_count) + " of " + str(total_trips))
		
	iter_count +=1
		
	#add bike to new its new station location
	#and adjust data on all other bikes there.	
	if(len(stations[end_sid]) != 0):
		for bike in stations[end_sid]:	
			if(new_entry.date_arrived > stations[end_sid][bike].date_arrived):
				stations[end_sid][bike].ins += 1
	
	if(last_seen[bike_id] == None):
		stations[end_sid][bike_id] = new_entry		
	else:
		prev_station = stations[last_seen[bike_id]]
		#remove the bike from station and update
		#data for other bikes.
		bike =  prev_station[bike_id]	
		out_trips =	bike.outs
		in_trips = bike.ins
		del	stations[last_seen[bike_id]][bike_id]

		for bike in prev_station:
			if(start_time > prev_station[bike].date_arrived):
				prev_station[bike].outs += 1	
		last_station = last_seen[bike_id]
		if(last_station not in capacities):
			station_names_not_found.write(last_station+"\n")
			station_error.add(last_station)
		else:
			hidden_trip = (last_station != start_sid)	
			#calculate the probability that the bike was broken
			stat_cap = capacities[last_station]
			if(stat_cap > 0):
				prob_broken = PRIOR_BROKEN / (PRIOR_BROKEN + ((stat_cap - 1)/float(stat_cap))**out_trips * (1 - PRIOR_BROKEN))
				#prob_broken_in_out = PRIOR_BROKEN / (PRIOR_BROKEN + ((stat_cap - 1)/float(stat_cap))**(out_trips-in_trips) * (1 - PRIOR_BROKEN))
			else:
				print last_station + " has < 1 capacity."
			if(hidden_trip):
				trip_type = "HIDDEN"
			else:
				trip_type = "NORMAL"
			trip_entry = (duration, start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id, prob_broken, trip_type)
			trip_list.append(trip_entry)	

		stations[end_sid][bike_id] = new_entry
		
	last_seen[bike_id] = end_sid


out_file = csv.writer(open(DATA_OUT, "w"))
out_header = ["time_delta", "bike_id", "last_station", "date", "in_since_date", "out_since_date"]
out_file.writerow(out_header)

#make one big list of tuples to output
final_list = []
last_time_record = datetime.strptime(LAST_DATE, FMT)

logMsg("Creating final list.")
for key in stations:
	calculate_prob = False
	if(key not in station_error):
		calculate_prob = True
		stat_cap = capacities[key]

	if(len(stations[key]) != 0):
		for bike in stations[key]:
			curr_bike = stations[key][bike]
			last_date_seen = datetime.strptime(curr_bike.date_arrived, FMT)
			time_delta = last_time_record - last_date_seen
			time_delta = time_delta.total_seconds()
			
			entry = (time_delta, curr_bike.id, curr_bike.last_station, curr_bike.date_arrived, curr_bike.ins, curr_bike.outs)
			final_list.append(entry)
			if(calculate_prob):		
				prob_broken = PRIOR_BROKEN / (PRIOR_BROKEN + ((stat_cap - 1)/float(stat_cap))**curr_bike.outs * (1 - PRIOR_BROKEN))
				broke_entry = (time_delta, last_date_seen, last_time_record, curr_bike.last_station, key, curr_bike.last_station, key, curr_bike.id, prob_broken, "END")
				trip_list.append(broke_entry)	

logMsg("Sorting final list and writing to output file.")
final_list.sort(reverse = True)
for entry in final_list:
	out_file.writerow(entry)	

logMsg("Writing out probability file.")
trip_list.sort(key = lambda x : x[-2])
out_file_prob = csv.writer(open(PROB_OUT, "w"))
prob_header = ["duration", "start_time", "end_time", "start_station", "start_sid", "end_station", "end_sid", "bike_id", "prob_broken", "trip_type"]
out_file_prob.writerow(prob_header)

for trip in trip_list:
	out_file_prob.writerow(trip)

logMsg("Done.")
