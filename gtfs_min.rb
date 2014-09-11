#!/usr/bin/ruby

require 'sqlite3'
require 'fileutils'

def getStopsForRoute(route, direction)
    stops = @old_db.execute <<-eos
		SELECT stop_times.stop_id, stop_times.stop_sequence, trips.trip_headsign 
		FROM trips 
		JOIN stop_times ON trips.trip_id = stop_times.trip_id AND route_id = '#{route}' AND direction_id = '#{direction}' 
		JOIN stops ON stop_times.stop_id = stops.stop_id 
		GROUP BY stops.stop_id, stops.stop_name;
    eos
end

def getRoutes
	routes = @old_db.execute "select route_id from routes;"
end

begin	
	@orig_db_name = "mbta.db"
	@min_db_name = "mbta_min.db"

	puts "Creating mbta_min.db"
	FileUtils.cp(@orig_db_name, @min_db_name)

	@old_db = SQLite3::Database.open @orig_db_name
	@new_db = SQLite3::Database.open @min_db_name

	@old_db.results_as_hash = true
	@new_db.results_as_hash = true

	puts "Creating route_stops table"
	@new_db.execute <<-eos
		CREATE TABLE route_stops(
			route_id varchar(11) DEFAULT(NULL),
   			direction_id tinyint(1) DEFAULT(NULL),
   			stop_id varchar(11) DEFAULT(NULL),
   			stop_sequence int(11) DEFAULT(NULL),
   			trip_headsign varchar(255) DEFAULT(NULL)
		);
	eos

	puts "Adding route_stops"
	routes = getRoutes
	routes.each do |route|
		for i in 0..1
			route_id = route['route_id']
			puts route_id
			stops = getStopsForRoute(route_id, i.to_s)
			stops.each do |stop|
				insert = @new_db.prepare <<-eos
					INSERT INTO route_stops 
						(route_id, direction_id, stop_id, stop_sequence, trip_headsign) 
					VALUES 
						('#{route_id}', #{i.to_s}, '#{stop['stop_id']}', #{stop['stop_sequence']}, ?);
				eos
				insert.bind_param(1, stop['trip_headsign'])
				insert.execute
			end
		end
	end

	puts "Creating route_id_direction_id index on route_stops"
	@new_db.execute "CREATE INDEX route_id_direction_id on route_stops (route_id, direction_id);"

	puts "Dropping stop_times table"
	@new_db.execute "DROP TABLE stop_times;"
	puts "Dropping trips table"
	@new_db.execute "DROP TABLE trips;"
	puts "Vacuuming database"
	@new_db.execute "VACUUM;"
	puts "All done!"

rescue SQLite3::Exception => e 
    puts "Exception occured"
    puts e.backtrace
    puts e	 
ensure
    @old_db.close if @old_db
    @new_db.close if @new_db
end
