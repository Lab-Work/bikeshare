"""
This is a simulation of occupancy states
of the Capital Bikeshare stations. As input
it takes the regular trip list and a proposed
hidden trip list. The output is based on the 
times when a station goes over capacity. This 
information includes the station, the date and
time that it occured as well as the probabilistic
states of all of the bikes at the station at 
that time. The broken probabilities must be supplied.

Author: Chase Duncan 
"""

import pdb
import csv
import sys
from Queue import PriorityQueue
from collections import defaultdict
from datetime import datetime
from tools import *

DEBUG = False
REMOVE_BB = True

STATION_INFO = "/Users/chaseduncan/Desktop/capital/data/stations_info.csv"
class OccupancySimulator:
	
	def __init__(self, remove_bb):
		#a list for storing all of the occurences of
		#stations going over capacity.
		self.over_occ_list = []
		#holds a list of in events and out events
		#that occur as a result of hidden trips.
		self.hidden_events = None	
		#holds a list of incoming events and outgoing events
		#that occur as a result of regular trips.
		self.trip_events = None
		#dictionary that maps bike ids at certain times
		#to the probability that they are broken as well
		#as the trip type for which the bike was last used.
		self.broke_bikes = defaultdict()
		#dictionary that maps station ids to sets of bike_ids
		self.station_state = defaultdict(defaultdict)
		#dictionary of station capacities
		self.station_caps = defaultdict()
		#list of broken bike visits to be turned into hidden trips
		self.to_hidden = []
		#global date maintained to deal with collisions		 
		self.curr_date = defaultdict() 
		#attribute that tells us whether or not we are removing bikes in the simulation
		self.remove_bb_op = remove_bb
		#dict for maintaining regular in events that happen concurrently at a station
		self.concurrent_events = defaultdict()

		station_info = csv.reader(open(STATION_INFO, "r"))
		self.over_count = 0	
		self.ignore_stations = set()
		ig = open("/Users/chaseduncan/Desktop/capital/data/station_names_not_found.txt", "r")
		for line in ig:
			self.ignore_stations.add(line[:-1])

		for [station_id, station_name, terminal_name, install_date, latitude, longitude, capacity] in station_info:
			self.station_caps[terminal_name] = capacity
			self.curr_date[terminal_name] = None
			self.concurrent_events[terminal_name] = set()

	"""
	NESTED CLASSES
	"""
	#used to store information about the state
	#of a station when it goes over capacity
	class OverCapLog:
		def __init__(self, count, station, station_id, cap, date, bike_states, o_type):
			self.count = count
			self.station = station
			self.station_id = station_id
			self.cap = cap
			self.date = date
			self.bike_state_list = []
			self.o_type = o_type	
			for state in bike_states:
				self.bike_state_list.append(state)

	#nested class used for converting the hidden trip
	#file into sorted event lists.
	class HiddenEvents:
		def __init__(self, in_events, out_events):
			self.in_events = in_events
			self.out_events = out_events

	#nested class used for converting the regular trips
	#file into sorted event lists.
	class TripEvents:
		def __init__(self, in_events, out_events):
			self.in_events = in_events
			self.out_events = out_events

	#method that takes in the master trip list
	#and generates the trip_events lists for the
	#simulation object.
	def regular_trips_to_events(self, t_list):
		_list = csv.reader(open(t_list, "r"))
		#strip the header
		_list.next()
		in_events = []
		out_events = []

		for [start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id] in _list:
			in_entry = (end_time, end_station, end_sid, bike_id)				
			out_entry = (start_time, start_station, start_sid, bike_id)

			in_events.append(in_entry)	
			out_events.append(out_entry)
	
		in_events.sort()
		out_events.sort()

		self.trip_events =  OccupancySimulator.TripEvents(in_events, out_events)
		
	#method that takes in the hidden trip list
	def hidden_trips_to_events(self, h_list):
		_list = csv.reader(open(h_list, "r"))
		_list.next()
		in_events = []
		out_events = []

		for [from_station,from_sid,to_station,to_sid,bike_id,lower_bound,lower_bound_numeric,lower_date_id,upper_bound,upper_bound_numeric,upper_date_id,delta_t] in _list:
			in_entry = (upper_bound, upper_bound_numeric, upper_date_id, to_station, to_sid, bike_id, delta_t)				
			out_entry = (lower_bound, lower_bound_numeric, lower_date_id, from_station, from_sid, bike_id, delta_t)
			in_events.append(in_entry)	
			out_events.append(out_entry)
	
		in_events.sort()
		out_events.sort()

		self.hidden_events = OccupancySimulator.HiddenEvents(in_events, out_events)

	def generate_broke_bikes_dict(self, bike_file):
		bb_file = csv.reader(open(bike_file, "r"))
		bb_file.next()
		
		for line in bb_file:
			if len(line) > 10:
				line = line[1:]
			[start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id, out_trips, prob_broken,trip_type] = line
			key = (bike_id, start_time)
			self.broke_bikes[key] = (prob_broken, out_trips, start_time, end_time, start_station, start_sid, end_station, end_sid)

		
	def run_simulation(self):
		#variables for maintaining our position in each list
		h_in_pos = 0
		h_out_pos = 0
		t_in_pos = 0
		t_out_pos = 0	
	
		#sets the max size of the priority queue	
		MAX_SIZE = 4
		#index of event tuple that holds the date of the event  
		DATE_INDEX = 0
		
		#constants associate an integer with an event type
		T_IN = 0
		T_OUT = 3
		H_IN = 2
		H_OUT = 1
		
		#passed to evaluation function
		IN = 1
		OUT = -1
			
		pq = PriorityQueue(MAX_SIZE)	
		if(self.trip_events != None and self.hidden_events != None):
			pq.put((self.trip_events.in_events[t_in_pos][DATE_INDEX], T_IN))
			pq.put((self.trip_events.out_events[t_out_pos][DATE_INDEX], T_OUT))
			pq.put((self.hidden_events.in_events[h_in_pos][DATE_INDEX], H_IN))	
			pq.put((self.hidden_events.out_events[h_in_pos][DATE_INDEX], H_OUT))	

		while(not pq.empty()):
			#grab the identifying features of the event 
			#of the next greatest priority
			(event_date, event_id) = pq.get()

			if(event_id == T_IN):
				repop_pq = self.evaluate_regular_trip(self.trip_events.in_events[t_in_pos], IN)	
				t_in_pos += 1
				if(t_in_pos < len(self.trip_events.in_events)):
					pq.put((self.trip_events.in_events[t_in_pos][DATE_INDEX], T_IN))

			elif(event_id == T_OUT):
				repop_pq = self.evaluate_regular_trip(self.trip_events.out_events[t_out_pos], OUT)
				t_out_pos += 1
				if(t_out_pos < len(self.trip_events.out_events)):
					pq.put((self.trip_events.out_events[t_out_pos][DATE_INDEX], T_OUT))

			elif(event_id == H_IN):
				repop_pq = self.evaluate_hidden_trip(self.hidden_events.in_events[h_in_pos], IN)	
				h_in_pos += 1
				if(h_in_pos < len(self.hidden_events.in_events)):
					pq.put((self.hidden_events.in_events[h_in_pos][DATE_INDEX], H_IN))	

			elif(event_id == H_OUT):	
				repop_pq = self.evaluate_hidden_trip(self.hidden_events.out_events[h_out_pos], OUT)
				h_out_pos += 1
				if(h_out_pos == 47243):
					pdb.set_trace()
				if(h_out_pos < len(self.hidden_events.out_events)):
					pq.put((self.hidden_events.out_events[h_out_pos][DATE_INDEX], H_OUT))	
			
			logMsg("t_in_pos: " + str(t_in_pos) + " of " + str(len(self.trip_events.in_events)))
			logMsg("t_out_pos: " + str(t_out_pos) + " of " + str(len(self.trip_events.out_events)))
			logMsg("h_in_pos: " + str(h_in_pos) + " of " + str(len(self.hidden_events.in_events)))
			logMsg("h_out_pos: " + str(h_out_pos) + " of " + str(len(self.hidden_events.out_events)))
			logMsg("size of pq is: " + str(pq.qsize()))

			if(repop_pq):
				#dunno if I need to do this but doing it for safety
				pq = PriorityQueue(MAX_SIZE)
				if(t_in_pos < len(self.trip_events.in_events)):
					pq.put((self.trip_events.in_events[t_in_pos][DATE_INDEX], T_IN))
				if(t_out_pos < len(self.trip_events.out_events)):
					pq.put((self.trip_events.out_events[t_out_pos][DATE_INDEX], T_OUT))
				if(h_in_pos < len(self.hidden_events.out_events)):
					pq.put((self.hidden_events.in_events[h_in_pos][DATE_INDEX], H_IN))	
				if(h_out_pos < len(self.hidden_events.out_events)):
					pq.put((self.hidden_events.out_events[h_out_pos][DATE_INDEX], H_OUT))	

	def evaluate_regular_trip(self, trip, t_type):
		(time, station, sid, bike_id) = trip

		#if(time == '2011-04-28 08:48:00' and bike_id == 'W00714'):
			#pdb.set_trace()

		if(sid in self.ignore_stations):
			return False
		if(time != self.curr_date[sid]):
			self.concurrent_events[sid] = set()
			self.curr_date[sid] = time

		self.concurrent_events[sid].add(bike_id)

		if(t_type < 0):
			#for an out trip, simply remove the bike_id from the dict
			if bike_id in self.station_state[sid]:
				del self.station_state[sid][bike_id]
		else:
			#add bike_id to the set	
			add_bike = self.broke_bikes[(bike_id, time)]
			self.station_state[sid][bike_id] = (add_bike[0], add_bike[1], time, add_bike[3], add_bike[4], add_bike[5], add_bike[6], add_bike[7])
			#check if the station is over capacity 
			#to prevent multiple reportings of over capacitance
			#we only do this when the capacity is exactly over by one
			if(len(self.station_state[sid]) == int(self.station_caps[sid]) + 1):
				if(self.remove_bb_op):
					self.remove_bb(sid, bike_id)
					return True 

				self.over_count += 1
				#generate a list of bike states
				b_list = []
				for bike in self.station_state[sid]:
					if bike == bike_id:
						b_entry = ('*' + str(bike), self.station_state[sid][bike][0], self.station_state[sid][bike][1], self.station_state[sid][bike][2], self.station_state[sid][bike][3])	
					else:
						b_entry = (bike, self.station_state[sid][bike][0], self.station_state[sid][bike][1], self.station_state[sid][bike][2], self.station_state[sid][bike][3])	
					b_list.append(b_entry)
				#make an overcapacity log
				ocl = self.OverCapLog(self.over_count, station, sid, self.station_caps, time, b_list, 'REGULAR')	
				self.over_occ_list.append(ocl)
		
		return False

	def evaluate_hidden_trip(self, trip, t_type):
		(date, date_numeric, date_id, station, sid, bike_id, delta_t) = trip

		#if(date == '2011-04-28 08:48:00' and bike_id == 'W00714'):
			#pdb.set_trace()

		if(sid in self.ignore_stations):
			return

		if(t_type < 0):
			#for an out trip, simply remove the bike_id from the dict
			if bike_id in self.station_state[sid]:
				del self.station_state[sid][bike_id]
		else:
			#add bike_id to the set	
			self.station_state[sid][bike_id] = ('Unknown', 'Unknown', date, 'Unknown')
			#check if the station is over capacity 
			#to prevent multiple reportings of over capacitance
			#we only do this when the capacity is exactly over by one
			if(len(self.station_state[sid]) == int(self.station_caps[sid]) + 1):
				if(self.remove_bb_op):
					self.remove_bb(sid, bike_id)
					return True

				self.over_count += 1
				#generate a list of bike states
				b_list = []
				for bike in self.station_state[sid]:
					if bike == bike_id:

						b_entry = ('*' + str(bike), self.station_state[sid][bike][0], self.station_state[sid][bike][1], self.station_state[sid][bike][2], self.station_state[sid][bike][3])	
					else:
						b_entry = (bike, self.station_state[sid][bike][0], self.station_state[sid][bike][1], self.station_state[sid][bike][2], self.station_state[sid][bike][3])	

					b_list.append(b_entry)
				#make an overcapacity log
				ocl = self.OverCapLog(self.over_count, station, sid, self.station_caps, date, b_list, 'HIDDEN')	
				self.over_occ_list.append(ocl)

		return False

	def remove_bb(self, sid, new_bike):
		logMsg("Removing bike.")
		greatest_prob = 0
		b_bike = None
		station_state = self.station_state[sid]
		#logMsg("The size of self.concurrent_events is " + str(len(self.concurrent_events[sid])))
		for bike in station_state:
			if((station_state[bike][0]) == 'Unknown' or station_state[bike][2] == self.curr_date[sid]):
				continue
			
			if(float(station_state[bike][0]) > float(greatest_prob)):
				greatest_prob = station_state[bike][0]
				b_bike = bike

		#create hiddent trip entry for this visit and append to list.
		#(bike_id, prob, out_trips, start_time, end_time, start_station, start_sid, end_station, end_sid) = bike_to_remove
		if(b_bike == None):
			pdb.set_trace()
		
		r_bike = station_state[b_bike]
		entry = (r_bike[2], r_bike[3], r_bike[4], r_bike[5], r_bike[6], r_bike[7], b_bike)
		#entry = (start_time, end_time, start_station, start_sid, end_station, end_sid, bike_id)
		self.to_hidden.append(entry)
		#(date, date_numeric, date_id, station, sid, bike_id, delta_t)
		in_entry = (r_bike[3], None, None,  r_bike[6], r_bike[7], b_bike, None)
		out_entry = (r_bike[2], None, None, r_bike[4], r_bike[5], b_bike, None)

		self.hidden_events.in_events.append(in_entry)
		self.hidden_events.in_events.sort()
		#I don't think this part matters since we've already removed it from the station
			
		#self.hidden_events.out_events.append(out_entry)
		#self.hidden_events.out_events.sort()

		del station_state[b_bike]




















