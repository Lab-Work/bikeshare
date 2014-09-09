"""
Search for stations that are in 
the trip data but are not in the 
station capacity data.

Author: Chase Duncan
"""

import csv
import sys
from tools import *
from collections import defaultdict

#Input files
station_states = "/Users/chaseduncan/Desktop/capital/data/stations_info.csv"  
trip_data = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list.csv"

#set for holding station ids from the stations_info.csv file
ss_station_ids = set()

#dictionary for holding station ids and counts of occurences
#of those stations in the trip data. we count the frequency
#of the stations so that we have some feel for how important
#they are to the project. 
trip_data_ids = defaultdict(lambda : [0, 0])

#a dictionary for holding the date where we last saw a station
#in the trip data and a dictionary for holding where we first
#saw the station in the trip data.
first_seen = defaultdict(lambda : 0)
last_seen = defaultdict(lambda : 0)

#two sets for holding the stations that are seen in the trip
#data but not in the station states data and vice versa.
td_not_in_ss = set()
ss_not_in_td = set()

#two dictionaries for mapping station ids to station names.
#used for output.
ss_ids_to_stats = defaultdict()
td_ids_to_stats = defaultdict()

logMsg("Reading in data and creating sets of terminal ids.")

#open the two csv files and strip off the headers.
ss_file = csv.reader(open(station_states, "r"))
header = ss_file.next()
td_file = csv.reader(open(trip_data, "r"))
header = td_file.next()

#iterate through station_info.csv. add each station id (called terminal name) to the set
#of ids and map the id to the station name.
for [station_id, station_name, terminal_name, install_date, latitude, longitude, capacity] in ss_file:
	ss_station_ids.add(terminal_name)
	ss_ids_to_stats[terminal_name] = station_name

#iterate through the trip data and do as follows,
#1.increment frequency count
#2.map id to station
#3.if it's the first time we've seen this station then add it to first_seen
#4.if update last_seen
#note: this is done for both ends of the trip, i.e. start and end stations
for [duration,start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id,mem_type] in td_file:
	#origin bookkeeping
	trip_data_ids[start_sid][0] += 1
	td_ids_to_stats[start_sid] = start_station

	if(first_seen[start_sid] == 0):
		first_seen[start_sid] = start_time
	if(start_time > last_seen[start_sid]):
		last_seen[start_sid] = start_time

	#destination bookkeeping
	trip_data_ids[end_sid][1] += 1
	ss_ids_to_stats[end_sid] = end_station

	if(first_seen[end_sid] == 0):
		first_seen[end_sid] = end_time
	if(end_time > last_seen[end_sid]):
		last_seen[end_sid] = end_time

#check the station set and trip dict against each other
#and note differences.
logMsg("Checking for ids that are in master_trip_list.csv but not in stations_info.csv.")
for _id in trip_data_ids:
	if(_id not in ss_station_ids):
		td_not_in_ss.add(_id)

logMsg("Checking for ids that are in stations_info.csv but not in master_trip_list.csv.")
for _id in ss_station_ids:
	if(_id not in trip_data_ids):
		ss_not_in_td.add(_id)

#set difference is taken for redundancy.
s_diff = td_not_in_ss.symmetric_difference(ss_not_in_td)

#output all of this data in the various ways it needs to be put out.
logMsg("Writing data out.")
outfile = open("/Users/chaseduncan/Desktop/capital/data/missing_stations.txt", "a")
outfile.write("Stations in master but not in info.\n")
count = 1
for sid in td_not_in_ss:
	if(len(sid) != 0):
		outfile.write(str(count)+". " + sid + " " + td_ids_to_stats[sid] + " seen in " + str(trip_data_ids[sid]) + " trips." + "Last seen on " + last_seen[sid] +"\n")
		count += 1

outfile.write("\n Stations in info but not in master.\n")
count = 1
for sid in ss_not_in_td:
	outfile.write(str(count) + ". " + sid + " "+ ss_ids_to_stats[sid] + "\n")
	count += 1

#make a list of tuples that contain (station_id, station_name, first_seen, last_seen, in_trips, out_trips, total_trips)
station_list = []
for sid in trip_data_ids:
	if(len(sid) != 0):
		total_trips = trip_data_ids[sid][0] + trip_data_ids[sid][1]
		entry  = (sid, td_ids_to_stats[sid], first_seen[sid], last_seen[sid], trip_data_ids[sid][0], trip_data_ids[sid][1], total_trips)
		station_list.append(entry)

#sort list by station_id and write it out to file
stations_out = csv.writer(open("/Users/chaseduncan/Desktop/capital/data/station_usage_stats.csv", "w"))
header = ["station_id", "station_name", "first_seen", "last_seen", "in_trips", "out_trips"]
stations_out.writerow(header)

station_list.sort(key = lambda x: x[-1], reverse = True)

for entry in station_list:
	stations_out.writerow(entry)

