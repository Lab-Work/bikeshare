"""
This file was created to run the simulation.
It will probably serve well as a template
but one will want to write their own based
on the future needs of the project.

Author: Chase Duncan
Contact: cddunca2@illinois.edu
"""

import csv
from occupancy_simulator import *
from tools import *

#This flag determines whether or not we remove broken_bikes from stations
#and make them hidden trips.
REMOVE_BB = False
HIDDEN_FILE = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/hidden_trips.csv"
HIDDEN_FILE_EX = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/hidden_trips_ex.csv"
TRIP_FILE = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/master_trip_list_clean.csv"
BROKEN_OUT ="/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_visits.csv"

logMsg("Creating a new simulator object.")
os = OccupancySimulator(REMOVE_BB)

logMsg("Reading in regular trips.")
os.regular_trips_to_events(TRIP_FILE)
logMsg("Reading in hidden trips.")
os.hidden_trips_to_events(HIDDEN_FILE_EX)

logMsg("Generating broken bikes dictionary.")
os.generate_broke_bikes_dict("/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_probs.csv")
logMsg("Running simulation.")
os.run_simulation()

if(REMOVE_BB):
	logMsg("Writing out broken visit file.")
	broken_out = csv.writer(open(BROKEN_OUT, "w"))
	header = ["start_time", "end_time", "start_station", "start_sid", "end_station", "end_sid", "prob_broke", "bike_id"]
	broken_out.writerow(header)
	for line in os.to_hidden:
		broken_out.writerow(line)
		
	
		
out_file = "/Users/chaseduncan/Desktop/capital/data/simulator_output.txt"

logMsg("Generation output file.")

if(len(os.over_occ_list) == 0):
	print "There were no occurences of over occupancy."
else:
	with open(out_file, "w") as f:
		for log in os.over_occ_list:
				f.write("**********************")
				f.write("Over cap log #" + str(log.count) + "\n")
				f.write("Station " + log.station + " went over capacity on " + log.date +" because of "+log.o_type+ " trip.\n")
				f.write("The broken bike probabilities were as follows,\n")
				for bike in log.bike_state_list:
					f.write("Bike: " + str(bike[0]) + " Probability broken: " + str(bike[1]) + " Outs: " + str(bike[2]) + " Arrived: " + str(bike[3]) + " Leaves: " + str(bike[4]) +"\n") 	
				f.write("\n")
logMsg("Done.")






