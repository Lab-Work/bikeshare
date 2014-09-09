"""
An attempt to find broken bikes
in the Capital Bikeshare system.

Author: Chase Duncan
Contact: cddunca2@illinois.edu
"""
import pdb
import csv
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from tools import *

#path to input file
DATA_IN = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list_clean.csv"
#this output was generated for the sake of testing and debugging
DATA_OUT = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/bikes_last_seen.csv"
#this is the important output from this code.
PROB_OUT =  "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_probs.csv"

#Last time in the dataset. 
END_OF_TIME = "2014-04-01 00:06:00"
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
#used to map station ids to names.
id_to_name = defaultdict()
trip_list = []
#this list holds stations for which we do not have a capacity
station_error = set()
#dictionary for looking up capacities of stations
capacities = defaultdict()

#get the capacities of the stations
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

#A patch. Once we removed trips that start and end at the same station, at the same time, the logic
#we used to remove stations that aren't in the capacities list no longer worked for this station.
#The station in question is the Alta Tech Office for which there were two trips in the entire dataset.
#It's irrelevant.
station_error.add('32902')

for [start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id] in in_file:

	#get mappint of id to name
	id_to_name[start_sid] = start_station
	id_to_name[end_sid] = end_station
	
	#make a new bike object for this trip
	new_entry = Bike(bike_id)
	new_entry.last_station = end_station
	new_entry.last_sid = end_sid
	new_entry.date_arrived = end_time

	#output progress message as necessary
	if(iter_count % 10000 == 0):
		logMsg("Processing trip " + str(iter_count) + " of " + str(total_trips))
		
	iter_count +=1
		
	#adjust data on all bikes at the destination station of the trip.
	for bike in stations[end_sid]:	
		if(new_entry.date_arrived > stations[end_sid][bike].date_arrived):
			stations[end_sid][bike].ins += 1
	
	#did it come from somewhere? if no, the we simply add the bike to its
	#new station. This is the case at the start of the data set.
	if(last_seen[bike_id] == None):
		stations[end_sid][bike_id] = new_entry		

	#if the bike came from another station then we have work to do.
	else:
		prev_station = stations[last_seen[bike_id]]
		#remove the bike from station and update
		#data for other bikes.
		bike =  prev_station[bike_id]	
		out_trips =	bike.outs
		in_trips = bike.ins
		del	stations[last_seen[bike_id]][bike_id]

		for b in prev_station:
			if(start_time > prev_station[b].date_arrived):
				prev_station[b].outs += 1	

		last_station = last_seen[bike_id]

		if(last_station not in capacities):
			station_names_not_found.write(last_station+"\n")
			station_error.add(last_station)

		else:
			hidden_trip = (last_station != start_sid)	
			#calculate the probability that the bike was broken
			stat_cap = capacities[last_station]
			prob_broken = PRIOR_BROKEN / (PRIOR_BROKEN + ((stat_cap - 1)/float(stat_cap))**out_trips * (1 - PRIOR_BROKEN))

			#this field is for knowing if a bike is moved during its visit
			if(hidden_trip):
				trip_type = "HIDDEN"
			else:
				trip_type = "NORMAL"

			trip_entry = (bike.date_arrived, start_time, id_to_name[last_station], last_station, start_station, start_sid, bike_id, out_trips, prob_broken, trip_type)
			trip_list.append(trip_entry)	

		stations[end_sid][bike_id] = new_entry
		
	last_seen[bike_id] = end_sid


out_file = csv.writer(open(DATA_OUT, "w"))
out_header = ["time_delta", "bike_id", "last_station", "date", "in_since_date", "out_since_date"]
out_file.writerow(out_header)

#make one big list of tuples to output
final_list = []
last_time_record = datetime.strptime(END_OF_TIME, FMT)

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
				prob_broken = PRIOR_BROKEN / (PRIOR_BROKEN + ((stat_cap - 1)/float(stat_cap))**(curr_bike.outs + 20) * (1 - PRIOR_BROKEN))
				broke_entry = (last_date_seen, last_time_record, curr_bike.last_station, key, curr_bike.last_station, key, curr_bike.id, curr_bike.outs, prob_broken, "END")
				trip_list.append(broke_entry)	

logMsg("Sorting final list and writing to output file.")
final_list.sort(reverse = True)
for entry in final_list:
	out_file.writerow(entry)	

logMsg("Writing out probability file.")
trip_list.sort(key = lambda x : x[-2])
out_file_prob = csv.writer(open(PROB_OUT, "w"))
prob_header = ["start_time", "end_time", "start_station", "start_sid", "end_station", "end_sid", "bike_id", "out_trips", "prob_broken", "trip_type"]
out_file_prob.writerow(prob_header)

for trip in trip_list:
	out_file_prob.writerow(trip)

logMsg("Done.")
