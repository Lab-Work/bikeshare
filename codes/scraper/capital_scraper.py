"""
Scrapes the Capital Bikeshare website for 
station state data. Appends to csv.

Author: Chase Duncan
"""

import csv
import sys
import mechanize
import time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

#URL to read from.
URL = "https://www.capitalbikeshare.com/data/stations/bikeStations.xml"

#reads in xml file from URL and writes it 
#out locally to be parsed.
def read_xml():
	br = mechanize.Browser()
	response = br.open(URL)

	temp_file = open("bike_stations_temp.xml", "w")
	temp_file.write(response.read())

#parse the xml that is read in
#by read_xml(). appends to csv.
def parse_file():
	#List to hold tuples of station info.
	station_list = []

	#Extract xml file.
	tree = ET.parse("bike_stations_temp.xml")
	root = tree.getroot()
	read_time = int(root.attrib['lastUpdate']) / 1000

	#Iterate over children of tree and extract info. Make a tuple
	#and add it to the list. I might be doing this in
	#a weird way but it was the most obvious approach to me.
	for child in root:
		station_id = int(child[0].text)
		station_name = child[1].text
		terminal_name = child[2].text
		bikes = int(child[12].text)
		empty_docks = int(child[13].text)
		last_update = int(child[3].text) /1000

		entry = (station_id, station_name, terminal_name, bikes, empty_docks, last_update, read_time)
		station_list.append(entry)

	#Write the csv file for output.
	f = open("capital_station_states.csv", "a")
	out_file = csv.writer(f)

	for station in station_list:
		out_file.writerow(station)

def main():
	while(True):
		start = datetime.now()
		read_xml()	
		parse_file()
		end = datetime.now()
			
		time_diff = end - start
		sleep_time = 60 - time_diff.total_seconds()

		time.sleep(sleep_time)

if __name__ == "__main__":
	main()











