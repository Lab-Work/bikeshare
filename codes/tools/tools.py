# -*- coding: utf-8 -*-
"""
A few utility methods used by various other files.

Created on Sat May  3 12:33:42 2014

@author: brian
"""
from datetime import datetime
import math

program_start = datetime.now()
#A convenient print statement for long runs - also includes a timestamp at the beginning of the message
#Arguments:
	#msg - a string to be printed
def logMsg(msg):
	td = datetime.now() - program_start
	print "[" + str(td) + "]  " + msg


#A print statement intended to log the percentage of completion of some task with many iterations
#Can be called many times, but only prints when the percentage is a "nice" number, rounded to a given number of digits
#Arguments
	#num - the current iteration
	#outof - the total number of iterations
	#How many digits should the percentage be rounded to?
def logPerc(num, outof, digits):
	rounded = round(float(num)/outof, digits)
	
	prev = round(float(num-1)/outof, digits)
	
	if(prev < rounded):
		logMsg(str(rounded*100) + "%")


#helper function. Computes euclidean distance between two vectors
def euclideanDist(v1, v2):
	s = 0
	for i in range(len(v1)):
		s += (v1[i] - v2[i]) **2
	return math.sqrt(s)
	

EARTH_RADIUS = 3963.1676
#computes distance between two lat-lon points, assuming spherical earth
def haversine((lat1,lon1), (lat2,lon2)):
	[lat1, lon1, lat2, lon2] = map(math.radians, [lat1, lon1, lat2, lon2])
	lat_haversine = math.sin((lat2-lat1)/2) * math.sin((lat2-lat1)/2)
	lon_haversine = math.sin((lon2 - lon1)/2) * math.sin((lon2 - lon1)/2)
	cosine_term = math.cos(lat1) * math.cos(lat2)
	distance = 2 * EARTH_RADIUS * math.asin(math.sqrt(lat_haversine + cosine_term*lon_haversine))
	return distance

#helper function. Normalizes a vector in-place
def normalize(vector):
	s = sum(vector)
	for i in range(len(vector)):
		vector[i] = float(vector[i]) / s
		

#A builder function - yields a squence of datetime objects
#Arguments:
	#start_date - a datetime object. the first date of the sequence
	#end_date - a datetime object. the end of the date sequence (non inclusive)
	#delta - a timedelta object.  The step size
def dateRange(start_date, end_date, delta):
	d = start_date
	while(d < end_date):
		yield d
		d += delta

#Rounds a datetime to a given granularity (1 hour, 15 minutes, etc..)
#Arguments
	#dt - a datetime object
	#granularity - a timedelta object
#Returns a datetime, rounded to the given granularity
def roundTime(dt, granularity):
	start_time = datetime(year=2000,month=1,day=1,hour=0)	
	
	tmp = dt - start_time
	
	rounded = int(tmp.total_seconds() / granularity.total_seconds())
	
	return start_time + rounded*granularity


#Takes a list, which represents the header row of a table
#Returns a dictionary which maps the string column names to integer column ids
def getHeaderIds(header_row):
	mapping = {}
	for i in range(len(header_row)):
		mapping[header_row[i]] = i

def time_search(time_list, time):
	
	lo = 0
	hi = len(time_list) - 1

	mid = (lo + hi) / 2

	while( lo != hi and lo != mid ):
		if(time < time_list[mid]):
			hi = mid
		else:
			lo = mid

		mid = (hi + lo) / 2	
		
	return hi
"""	
	if(lo == hi):
		return lo
	else:
		lo_dist = time_dist(time_list[lo], time)
		hi_dist = time_dist(time_list[hi], time)

		if(lo_dist < hi_dist):
			return lo
		else:
			return hi
"""
#Returns the magnitude in seconds of the difference between
#two datetime objects.
def time_dist(time_1, time_2):
	delta = time_1 - time_2
	delta = abs(delta.total_seconds())
	return delta

