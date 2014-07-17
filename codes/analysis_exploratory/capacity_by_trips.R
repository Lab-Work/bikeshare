DATA_DIR = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/relative_station_occupancies/"
HID_TRIP = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/hidden_trips.csv"
STAT_USE_FILE = "/Users/chaseduncan/Desktop/capital/data/station_usage_stats.csv"

#hidden_trips = read.csv(HID_TRIP)
stat_use = read.csv(STAT_USE_FILE)

pdf("Relative_Station_Occupancies.pdf", 100, 20)

#stations = c("31007", "31015", "31021", "31025", "31054")
for(sid in unique(stat_use$station_id)){
	#construct path to file
	data_dir = paste(DATA_DIR, sid, ".csv", sep ="")
	station_occ = read.csv(data_dir)
	
	name = stat_use[stat_use$station_id == sid, ]$station_name	
	plot(station_occ$timestamp, station_occ$occupancy, xlab = "Time", ylab = "Occupancy", main = paste(name, "/", sid, sep = " "))
	lines(station_occ$timestamp, station_occ$occupancy, lwd = 1, type = "s")

}

dev.off()


















