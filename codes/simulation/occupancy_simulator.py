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
Contact: cddunca2@illinois.edu
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
END_OF_TIME = '2014-04-01 00:06:00'
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
			#if(station_name == 'null station'):
				#pdb.set_trace()
			self.station_caps[terminal_name] = capacity
			self.curr_date[terminal_name] = None

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
			self.in_add_events = []

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
	#and converts each hidden trip into two
	#separate events.
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

	#takes in the broken bikes probability file
	#and creates a dictionary that maps a start
	#time of a visit and its associate bike id
	#to the info in the broken bikes file.
	def generate_broke_bikes_dict(self, bike_file):
		bb_file = csv.reader(open(bike_file, "r"))
		bb_file.next()
		
		for line in bb_file:
			if len(line) > 10:
				line = line[1:]
			[start_time,end_time,start_station,start_sid,end_station,end_sid,bike_id, out_trips, prob_broken,trip_type] = line
			key = (bike_id, start_time)
			self.broke_bikes[key] = (prob_broken, out_trips, start_time, end_time, start_station, start_sid, end_station, end_sid)

	#runs the simulation. 
	def run_simulation(self):
		#variables for maintaining our position in each list
		h_in_pos = 0
		h_out_pos = 0
		t_in_pos = 0
		t_out_pos = 0	
		h_in_add_pos = 0	

		#sets the max size of the priority queue	
		MAX_SIZE = 5
		#index of event tuple that holds the date of the event  
		DATE_INDEX = 0
		
		#constants associate an integer with an event type
		#these are used for event prioritization
		T_IN = 0 #regular trip in
		T_OUT = 3 #regulare trip out
		H_IN = 2 #hidden trip in
		H_OUT = 1 #hidden trip out
			
		#passed to evaluation function to differentiate
		#between an in or out event respectively
		IN = 1
		OUT = -1
			
		#used for updating user as to progress of simulation
		log_count = 0

		#create and initialize the priority queue
		pq = PriorityQueue(MAX_SIZE)	
		if(self.trip_events != None and self.hidden_events != None):
			pq.put((self.trip_events.in_events[t_in_pos][DATE_INDEX], T_IN))
			pq.put((self.trip_events.out_events[t_out_pos][DATE_INDEX], T_OUT))
			pq.put((self.hidden_events.in_events[h_in_pos][DATE_INDEX], H_IN))	
			pq.put((self.hidden_events.out_events[h_in_pos][DATE_INDEX], H_OUT))	

		"""
		How the Simulation Works:

		The following loop is where the bulk of the work takes place. We use our
		priority queue as a way to cache the sorting on the fly as opposed to having
		to sort (and resort) the rather large event lists.

		Priority is given first to date. Then, in the case of ties (for which there
		are many), we look at the 'type' of event it is. Priority is given as follows,
		T_IN > H_OUT > H_IN >T_IN.

		So, the loop gets the next element from the queue and then based on what type
		of event it is, we choose how to process it. IF a new hidden_trip is added, i.e.
		a broken bike has been removed from a station, then the queue must be repopulated.

		Also, as the simulation progresses, we must consider the two different types of
		hidden trips. Namely, those we which have originally and those which we add during
		the course of the simulation. This is because they are in different lists and we 
		must make sure to increment the proper indexing variable.
		"""
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
				if(h_in_pos < len(self.hidden_events.in_events) and self.hidden_events.in_events[h_in_pos][DATE_INDEX] == event_date):
					repop_pq = self.evaluate_hidden_trip(self.hidden_events.in_events[h_in_pos], IN)	
					h_in_pos += 1
					if(h_in_pos < len(self.hidden_events.in_events)):
						pq.put((self.hidden_events.in_events[h_in_pos][DATE_INDEX], H_IN))	
				else:
					repop_pq = self.evaluate_hidden_trip(self.hidden_events.in_add_events[h_in_add_pos], IN)
					h_in_add_pos += 1
					if(h_in_add_pos < len(self.hidden_events.in_add_events)):
						pq.put((self.hidden_events.in_add_events[h_in_add_pos][DATE_INDEX], H_IN))

			elif(event_id == H_OUT):	
				repop_pq = self.evaluate_hidden_trip(self.hidden_events.out_events[h_out_pos], OUT)
				h_out_pos += 1
				if(h_out_pos < len(self.hidden_events.out_events)):
					pq.put((self.hidden_events.out_events[h_out_pos][DATE_INDEX], H_OUT))	

			if(log_count % 10000 == 0):			
				logMsg("t_in_pos: " + str(t_in_pos) + " of " + str(len(self.trip_events.in_events)))
				logMsg("t_out_pos: " + str(t_out_pos) + " of " + str(len(self.trip_events.out_events)))
				logMsg("h_in_pos: " + str(h_in_pos) + " of " + str(len(self.hidden_events.in_events)))
				logMsg("h_out_pos: " + str(h_out_pos) + " of " + str(len(self.hidden_events.out_events)))
				logMsg("h_in_add_pos: " + str(h_in_add_pos) + " of " + str(len(self.hidden_events.in_add_events)))

			#if an additional hidden event has been created, then we must repopulate
			#the priority queue
			if(repop_pq):
				pq = PriorityQueue(MAX_SIZE)
				if(t_in_pos < len(self.trip_events.in_events)):
					pq.put((self.trip_events.in_events[t_in_pos][DATE_INDEX], T_IN))
				if(t_out_pos < len(self.trip_events.out_events)):
					pq.put((self.trip_events.out_events[t_out_pos][DATE_INDEX], T_OUT))
				if(h_in_pos < len(self.hidden_events.in_events)):
					pq.put((self.hidden_events.in_events[h_in_pos][DATE_INDEX], H_IN))	
				if(h_out_pos < len(self.hidden_events.out_events)):
					pq.put((self.hidden_events.out_events[h_out_pos][DATE_INDEX], H_OUT))	
				if(h_in_add_pos < len(self.hidden_events.in_add_events)):
					pq.put((self.hidden_events.in_add_events[h_in_add_pos][DATE_INDEX], H_IN))

			log_count += 1
	
	"""
	The following processing functions analyze what happens when we add a bike to a station.
	"""
	
	#processes regular trip events
	def evaluate_regular_trip(self, trip, t_type):
		(time, station, sid, bike_id) = trip

		if(sid in self.ignore_stations):
			return False

		if(time != self.curr_date[sid]):
			self.curr_date[sid] = time

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

	#processes hidden trip events
	def evaluate_hidden_trip(self, trip, t_type):
		(date, date_numeric, date_id, station, sid, bike_id, delta_t) = trip
		
		#if we don't have data on the capacity of the station, get out.
		if(sid in self.ignore_stations):
			return
		
		if(t_type < 0):
			#for an out trip, simply remove the bike_id from the dict
			if bike_id in self.station_state[sid]:
				del self.station_state[sid][bike_id]
		else:
			#add bike_id to the set	
			if(delta_t == 'Added'):
				self.station_state[sid][bike_id] = ('Unknown', 'Unknown', date, 'Added')
			else:
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

	#removes broken bikes from a station
	def remove_bb(self, sid, new_bike):
		
		greatest_prob = 0
		b_bike = None
		station_state = self.station_state[sid]
		for bike in station_state:
			if((station_state[bike][0]) == 'Unknown' or station_state[bike][2] == self.curr_date[sid] or station_state[bike][3] == self.curr_date[sid]):
				continue
			
			if(float(station_state[bike][0]) > float(greatest_prob)):
				greatest_prob = station_state[bike][0]
				b_bike = bike
		if(b_bike == None):
			pdb.set_trace()
		#create hiddent trip entry for this visit and append to list.
		#(bike_id, prob, out_trips, start_time, end_time, start_station, start_sid, end_station, end_sid) = bike_to_remove
		r_bike = station_state[b_bike]
		#we have to check if the hidden trip has a definite end or not
		#if it does not we set its end at the null station.
		if(r_bike[3] == END_OF_TIME):
			entry =  (r_bike[2], r_bike[3], r_bike[4], r_bike[5], 'null station', '00000', r_bike[0], b_bike)
			in_entry = (r_bike[3], None, None, 'null station', '00000', b_bike, 'Added')
		else:
			entry = (r_bike[2], r_bike[3], r_bike[4], r_bike[5], r_bike[6], r_bike[7], r_bike[0], b_bike)
			#entry = (start_time, end_time, start_station, start_sid, end_station, end_sid, prob_broke, bike_id)
			#(date, date_numeric, date_id, station, sid, bike_id, delta_t)
			in_entry = (r_bike[3], None, None,  r_bike[6], r_bike[7], b_bike, 'Added')

		self.to_hidden.append(entry)

		insert_index = 0 
		
		#all new hidden trips get added to 5th queue, in_hidden_add
		#we use a sort of insertion sort to put it in as opposed to 
		#adding it and resorting the entire list.
		if(len(self.hidden_events.in_add_events) == 0):
			self.hidden_events.in_add_events.append(in_entry)
		else:
			while(self.hidden_events.in_add_events[insert_index][0] < in_entry[0]):
				insert_index += 1
				if(insert_index >= len(self.hidden_events.in_add_events)):
					break
		
			self.hidden_events.in_add_events.insert(insert_index, in_entry)

		del station_state[b_bike]




















