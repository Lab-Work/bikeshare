"""
Script to merge all of the Capitol Bikeshare trip data csv files
into one big csv file

author: Chase Duncan
contact: cddunca2@illinois.edu
"""

import csv
import os
from datetime import datetime
from tools import *

#constants
DIR_PATH = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data"
DT_FMT = "%m/%d/%Y %H:%M"
TEST_PASS = False

#generate a list of all the csvs in the directory and sort it
logMsg("Generating file list.")
file_list = os.listdir(DIR_PATH)
file_list.sort()

#header for output file
master_header = [ "duration", "start_time", "end_time", "start_station", "start_sid", "end_station", "end_sid", "bike_id", "mem_type" ]

#create output file, open for writing and add header
master_dir = DIR_PATH + "/master_trip_list.csv"
output_file = csv.writer(open(master_dir, "w"))
output_file.writerow(master_header)

#first we put entries in a list so that we can sort them
out_list = []
#variable for output messages
index = 1

for file in file_list:
	#generate subdirectory path and open csv for reading
	subdir = DIR_PATH + "/" + file
	curr_file = csv.reader(open(subdir, "r"))
	
	if (subdir == master_dir or file == "headers.txt"):
		continue
	logMsg("Processing " + subdir + ". File number  " + str(index) + " of " + str(len(file_list)) + ".")
	index += 1

	#strip off the header
	header = curr_file.next()
	working_var = 1
	#iterate over the file and add each line to the output file
	#the files aren't formatted the same so it's necessary to 
	#to apply a slightly different parsing to subsets of the whole.
	#file_code holds the string needed to figure out which way to go.
	file_code = file[0:6]
	if(file_code[0] != '2'):
		print file_code[0]
		continue
	#parsing for 2010-1 and 2011-*
	if(file_code[:-2] == "2010" or file_code[:-2] == "2011"):
		for [duration, start_time, end_time, start_station, end_station, bike_id, mem_type] in curr_file:
			#for these files it's necessary to extract the station ids from
			#the station names. the following lines do so.
			start_sid = start_station[-6:-1]	
			end_sid = end_station[-6:-1]
			start_station = start_station[:-8]
			end_station = end_station[:-8]
			
			#convert times to datetime objects so we can sort by them			
			start_time = datetime.strptime(start_time, DT_FMT)
			end_time = datetime.strptime(end_time, DT_FMT)	

			#add tuple to list
			entry = (duration, start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id, mem_type)
			out_list.append(entry)

			#generate update output as necessary
			if(working_var % 15000 == 0):
				logMsg("Still working on . Processing " + subdir + ". File number  " + str(index) + " of " + str(len(file_list)) + ".")

			working_var += 1
	
	#parsing for 2012-1 and 2012-2
	elif(file_code == "2012-1" or file_code == "2012-2"):	
		for [duration, dur_sec, start_time, start_station, start_sid, end_time, end_station, end_sid, bike_id, mem_type] in curr_file:
			#convert times to datetime objects so we can sort by them			
			start_time = datetime.strptime(start_time, DT_FMT)
			end_time = datetime.strptime(end_time, DT_FMT)	

			#add tuple to list
			entry = (duration, start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id, mem_type)
			out_list.append(entry)

			#generate update output as necessary
			if(working_var % 15000 == 0):
				logMsg("Still working on . Processing " + subdir + ". File number  " + str(index) + " of " + str(len(file_list)) + ".")

			working_var += 1
	#parsing for everything else
	else:
		for [duration, start_time, start_station, start_sid, end_time, end_station, end_sid, bike_id, mem_type] in curr_file:
			#convert times to datetime objects so we can sort by them			
			start_time = datetime.strptime(start_time, DT_FMT)
			end_time = datetime.strptime(end_time, DT_FMT)	

			#add tuple to list
			entry = (duration, start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id, mem_type)
			out_list.append(entry)

			#generate update output as necessary
			if(working_var % 15000 == 0):
				logMsg("Still working on . Processing " + subdir + ". File number  " + str(index) + " of " + str(len(file_list)) + ".")

			working_var += 1
	
	if(TEST_PASS):
		break

#sort the list
logMsg("Sorting trip list.")
out_list.sort(key = lambda x: x[1])

logMsg("Writing file out.")
for trip in out_list:
	output_file.writerow(trip)
logMsg("Done.")	
