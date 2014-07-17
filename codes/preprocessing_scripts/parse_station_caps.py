"""
This code parses the station state XML
from Capital Bikeshare. 

Input: XML file
Output: CSV file with columns station_id,
station_name, terminal_name, install_date, 
latitude, longitude, capacity

Author: Chase Duncan
"""

import csv
import xml.etree.ElementTree as ET
from datetime import datetime

"""
CONSTANTS
"""
INPUT = "/Users/chaseduncan/Desktop/capital/data/bikeStations.xml" 
OUTPUT = "/Users/chaseduncan/Desktop/capital/data/stations_info.csv"

"""
DATA STRUCTURES
"""
#List to hold tuples of station info.
station_list = []

#Extract xml file.
tree = ET.parse(INPUT)
root = tree.getroot()

#Iterate over children of tree and extract info. Make a tuple
#and add it to the list. I might be doing this in
#a weird way but it was the most obvious approach to me.
for child in root:
	station_id = int(child[0].text)
	station_name = child[1].text
	terminal_name = child[2].text
	install_date = datetime.utcfromtimestamp(int(child[8].text)/1000) 
	latitude = float(child[4].text)
	longitude = float(child[5].text)
	capacity = int(child[12].text) + int(child[13].text)
	
	entry = (station_id, station_name, terminal_name, install_date, latitude, longitude, capacity)
	station_list.append(entry)

#Write the csv file for output.
out_file = csv.writer(open(OUTPUT, "w"))
out_header = ("station_id", "station_name", "terminal_name", "install_date", "latitude", "longitude", "capacity")
out_file.writerow(out_header)

station_list.sort()
for station in station_list:
	out_file.writerow(station)












