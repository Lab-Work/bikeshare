filename = "/Users/chaseduncan/Desktop/capital/data/capital_trip_data/broken_probs.csv"
initial = read.csv(filename, nrows = 100)
classes = sapply(initial, class)
in_file = read.csv(filename, colClasses = classes)

png("Bike\ Conditions\ Post\ Trip.png", 1000 , 1000)
plot(in_file$prob_broken, type = "p")
dev.off()














