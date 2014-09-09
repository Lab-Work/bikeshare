"""
Script to merge all of the Capitol Bikeshare trip data csv files
into one big csv file

author: Chase Duncan

"""

import csv
import os
from tools import *

#constants
DIR_PATH = "/Users/chaseduncan/Desktop/capitol/capitol_trip_data"

#generate a list of all the csvs in the directory and sort it
logMsg("Generating file list.")
file_list = os.listdir(DIR_PATH)
file_list.sort()

#header for output file
master_header = [ "duration", "start_date", "end_date", "start_station", "end_station", "bike_id", "member_type" ]

#create output file, open for writing and add header
master_dir = DIR_PATH + "/master_trip_list.csv"
output_file = csv.writer(open(master_dir, "w"))
output_file.writerow(master_header)

#variable for output messages
index = 1

for file in file_list:
	#generate subdirectory path and open csv for reading
	subdir = DIR_PATH + "/" + file
	curr_file = csv.reader(open(subdir, "r"))
	
	if (subdir == master_dir):
		continue
	logMsg("Processing " + subdir + ". File number  " + str(index) + " of " + str(len(file_list)) + ".")
	index += 1

	#strip off the header
	header = curr_file.next()
	working_var = 1
	#iterate over the file and add each line to the output file
	for row in curr_file:
		if(working_var % 15000 == 0):
			logMsg("Still working on . Processing " + subdir + ". File number  " + str(index) + " of " + str(len(file_list)) + ".")

		working_var += 1
		output_file.writerow(row)
	







