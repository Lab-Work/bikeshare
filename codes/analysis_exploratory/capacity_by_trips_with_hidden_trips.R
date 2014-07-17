DATA_DIR = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/relative_station_occupancies/"
HID_TRIP = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/hidden_trips.csv"
STAT_USE_FILE = "/Users/chaseduncan/Desktop/capital/data/station_usage_stats.csv"
OUT_DIR = "/Users/chaseduncan/Desktop/capital/data/graphs"

hidden_trips = read.csv(HID_TRIP)
hidden_trips$to_sid = as.character(hidden_trips$to_sid)
stat_use = read.csv(STAT_USE_FILE)

stations = c("31007", "31015", "31021", "31025", "31054")
for(sid in unique(stat_use$station_id)){
#for(sid in stations){
	#construct path to input file and read it in
	data_dir = paste(DATA_DIR, sid, ".csv", sep ="")
	station_occ = read.csv(data_dir)
	#create the directory where we will write the PNGs
	out_dir = paste(OUT_DIR, sid, sep = "")	
	if(!file.exists(out_dir)){
		dir.create(out_dir)
	}

	#extract station name	
	name = stat_use[stat_use$station_id == sid, ]$station_name	
	
	#subsets of hidden trips
	in_hidden = hidden_trips[hidden_trips$to_sid  == sid, ]
	out_hidden = hidden_trips[hidden_trips$from_sid == sid, ]	

	for(date_id in unique(station_occ$date_id)){
		#generate filename for output	
		outfile = paste(OUT_DIR, sid, "/", date_id, ".png", sep="")
		subset = station_occ[station_occ$date_id == date_id,]
	
		sub_in_hidden = in_hidden[in_hidden$upper_date_id == date_id, ]
		sub_out_hidden = out_hidden[out_hidden$lower_date_id == date_id, ] 

		png(outfile, 3000, 2000)
		plot(as.POSIXct(subset$timestamp, tz = "EST", origin="1970-01-01"), subset$occupancy, xlab = "Time", ylab = "Occupancy", main = paste(name, "/", sid, "/", date_id, sep = " ", cex.lab=10, cex.axis=1.5, cex.main=1.5, cex.sub=1.5))
		lines(subset$timestamp, subset$occupancy, lwd = 1, type = "s")
		
		temp_1 = runif(nrow(sub_in_hidden), min = min(subset$occupancy), max = max(subset$occupancy))
		segments(x0 = sub_in_hidden$lower_bound_numeric, x1 = sub_in_hidden$upper_bound_numeric, y0 = temp_1, y1 = temp_1, col = "darkred", lwd = 4)
		temp_2 = runif(nrow(sub_out_hidden), min = min(subset$occupancy), max = max(subset$occupancy))
		segments(x0 = sub_out_hidden$lower_bound_numeric, x1 = sub_out_hidden$upper_bound_numeric, y0 = temp_2, y1 = temp_2, col = "darkgreen", lwd = 4)

		dev.off()
	}

}


warnings()
















