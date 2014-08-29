"""
This file was created as a part of the debugging
process. I don't remember what purpose it served.

"""
import csv
from collections import defaultdict
from tools import *
import sys

logMsg("Reading in files.")
broken_visits_file = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_visits.csv"
visits_file = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_probs.csv"

visits = csv.reader(open(visits_file, "r"))
header = visits.next()

broken_visits = csv.reader(open(broken_visits_file, "r"))

visits_dict = defaultdict()
visits_set = set()

broken_visits_set = set()

logMsg("Creating broken visits set.")
for b_visit in broken_visits:
	key = (b_visit[0], b_visit[7])
	broken_visits_set.add(key)

with open("duplicates.txt", "w") as f:
	logMsg("Creating visits dict and visits set.")
	for visit in visits:
		key = (visit[0], visit[6])
		if( key in visits_set):
			f.write(str(key) + "\n")
			
		visits_set.add(key)
		visits_dict[key] = visit
		
logMsg("Finding set difference.")
visits_set_diff = visits_set - broken_visits_set
 
logMsg("Writing output.")
output = csv.writer(open("/Users/chaseduncan/Desktop/capital/data/capital_trip_data/visits_difference.csv", "w"))
output.writerow(header)

for diff in visits_set_diff:
	output.writerow(visits_dict[diff])

logMsg("Done.")









