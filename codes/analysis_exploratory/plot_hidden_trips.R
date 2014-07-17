filename = "../preprocessing_scripts/hidden_trips.csv"
in_file = read.csv(filename)

pdf("Length_of_Hidden_Trips.pdf")
hist(in_file$delta_t / 3600, xlim = c(0, 250), breaks = 10000, xlab = "Hours", main = "Length of Hidden Trips")
dev.off()

in_file$lower_bound = as.character(in_file$lower_bound)

pdf("Hidden_Trips.pdf", 100, 5)

for(s_name in levels(in_file$from_station)){
	subset = in_file[in_file$from_station == s_name, ]
	print(s_name)
	plot(0,0, type = "n", xlim = range(in_file$lower_bound_numeric,in_file$upper_bound_numeric), ylim = c(1,10), main = s_name)
	temp = runif(nrow(subset), min = 1, max = 10)
	segments(x0 = subset$lower_bound_numeric, x1 = subset$upper_bound_numeric, y0 = temp, y1 = temp, lwd = 2)
}
 
dev.off()






