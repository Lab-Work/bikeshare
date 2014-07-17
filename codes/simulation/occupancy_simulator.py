"""
This is a simulation of occupancy states
of the Capital Bikeshare stations. As input
it takes the regular trip list and a proposed
hidden trip list. The output is based on the 
times when a station goes over capacity. This 
information includes the station, the date and
time that it occured as well as the probabilistic
states of all of the bikes at the station at 
that time. The broken probabilities must be supplied
as well.

Author: Chase Duncan 
"""

import csv
import sys
from collections import defaultdict
from datetime import datetime
from tools import *

class OccupancySimulator:
	
	def __init__(self):
		self.over_occ_list = []
				
	#nested class used for converting the hidden trip
	#file into sorted event lists.
	class HiddenEvents:
		def __init__(in_events, out_events):
			self.in_events = in_events
			self.out_events = out_events

		def hidden_trips_to_events(h_list):
			_list = csv.reader(open(h_list, "r"))
			in_events = []
			out_events = []

			for [from_station,from_sid,to_station,to_sid,bike_id,lower_bound,lower_bound_numeric,lower_date_id,upper_bound,upper_bound_numeric,upper_date_id,delta_t] in _list:
				in_entry = (upper_bound, upper_bound_numeric, upper_date_id, to_station, to_sid, bike_id, delta_t)				
				out_entry = (lower_bound, lower_bound_numeric, lower_date_id, from_station, from_sid, bike_id, delta_t)
				in_events.append(in_entry)	
				out_events.append(out_entry)
		
			in_events.sort()
			out_events.sort()

			retval = HiddenEvents(in_events, out_events)
			
			return retval












