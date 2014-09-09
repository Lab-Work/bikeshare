"""
This script is for generating a complete list of all bike_ids
in the trip file. Nothing to it.

Author: Chase Duncan

"""

"""
BROKEN
BBBBBBBBBBBBBBBBBBBBBROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN
ROKEN


"""
import csv
from tools import *

"""
CONSTANTS
"""
DATA_FILE = "/Users/chaseduncan/Desktop/capitol/data/capitol_trip_data/master_trip_list.csv"
OUT_FILE = "/Users/chaseduncan/Desktop/capitol/data/bike_ids.csv"
"""
DATA STRUCTURES
"""
#a set for all bike_ids
bike_ids_set = set()

#get the full trip file csv and strip header
trips = csv.reader(open(DATA_FILE, "r"))
header = trips.next()


logMsg("Generating set of ids.")
for [duration, start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id, member_type] in trips:
	#for each trip add the bike_id to the set
	bike_ids_set.add(str(bike_id))

#create a list of ids and sort it	
logMsg("Making list of ids and sorting.")
bike_ids_list = list(bike_ids_set)

#write the ids out to the file
logMsg("Writing output file.")
with open("bike_ids.txt", 'a') as f:
	for id in bike_ids_list:
		f.write(id + "\n")
	
logMsg("Done.")

