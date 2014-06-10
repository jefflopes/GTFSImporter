#!/usr/bin/ruby

require 'sqlite3'
require 'fileutils'

def getStopsForRoute(route, direction) 
    puts @old_db.execute <<-eos
    	SELECT '#{route}','#{direction}',stop_times.stop_id,stop_times.stop_sequence,stops.stop_name,stops.stop_lat,stops.stop_lon
		FROM stop_times 
		INNER JOIN stops  
		ON stop_times.stop_id=stops.stop_id 
		WHERE stop_times.trip_id IN (SELECT trip_id FROM trips WHERE route_id = '#{route}' AND direction_id = '#{direction}') 
		GROUP BY stop_times.stop_id,stop_times.stop_sequence
    eos
end

def getRoutes
	routes = @old_db.execute "select route_id from routes"
end

begin
	@orig_db_name = "mbta.db"
	@min_db_name = "mbta_min.db"

	FileUtils.cp(@orig_db_name, @min_db_name)

	@old_db = SQLite3::Database.open @orig_db_name
	@new_db = SQLite3::Database.open @min_db_name

	@new_db.execute "DROP TABLE calendar"
	@new_db.execute "DROP TABLE fare_attributes"
	@new_db.execute "DROP TABLE fare_rules"
	@new_db.execute "VACUUM"

	puts getStopsForRoute("933_", "1")
rescue SQLite3::Exception => e 
    puts "Exception occured"
    puts e	 
ensure
    @old_db.close if @old_db
    @new_db.close if @new_db
end

# INSERT INTO X.TABLE(Id, Value) SELECT * FROM Y.TABLE;
