#!/usr/bin/ruby

require 'sqlite3'
require 'fileutils'

def getStopsForRoute(route, direction)
    stops = @old_db.execute <<-eos
    	SELECT stop_times.stop_id,stop_times.stop_sequence
		FROM stop_times 
		INNER JOIN stops  
		ON stop_times.stop_id=stops.stop_id 
		WHERE stop_times.trip_id IN (SELECT trip_id FROM trips WHERE route_id = '#{route}' AND direction_id = '#{direction}') 
		GROUP BY stop_times.stop_id,stop_times.stop_sequence;
    eos
end

def getRoutes
	routes = @old_db.execute "select route_id from routes;"
end

begin
	@orig_db_name = "mbta.db"
	@min_db_name = "mbta_min.db"

	FileUtils.cp(@orig_db_name, @min_db_name)

	@old_db = SQLite3::Database.open @orig_db_name
	@new_db = SQLite3::Database.open @min_db_name

	@old_db.results_as_hash = true
	@new_db.results_as_hash = true

	@new_db.execute <<-eos
		CREATE TABLE route_stops(
			route_id CHAR(11) NOT NULL,
   			direction_id TINYINT(1) NOT NULL,
   			stop_id CHAR(11) NOT NULL,
   			stop_sequence INT(11) NOT NULL
		);
	eos

	routes = getRoutes
	routes.each do |route|
		for i in 0..1
			route_id = route['route_id']
			stops = getStopsForRoute(route_id, i.to_s)
			
			stops.each do |stop|
				@new_db.execute <<-eos
					INSERT INTO route_stops 
						(route_id, direction_id, stop_id, stop_sequence) 
					VALUES 
						('#{route_id}', #{i.to_s}, '#{stop['stop_id']}', #{stop['stop_sequence']});
				eos
			end
		end
	end

	@new_db.execute "DROP TABLE calendar;"
	@new_db.execute "DROP TABLE fare_attributes;"
	@new_db.execute "DROP TABLE fare_rules;"
	@new_db.execute "DROP TABLE stop_times;"
	@new_db.execute "DROP TABLE trips;"
	@new_db.execute "VACUUM;"
	
rescue SQLite3::Exception => e 
    puts "Exception occured"
    puts e	 
ensure
    @old_db.close if @old_db
    @new_db.close if @new_db
end

# INSERT INTO X.TABLE(Id, Value) SELECT * FROM Y.TABLE;
