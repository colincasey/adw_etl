module ETL #:nodoc:
  module Processor #:nodoc:
    class MissingWaypointsProcessor < ETL::Processor::Processor      
      def process
        control.sources.each_with_index do |s, i|
          configuration = s.configuration.merge({ :enumerable => fill_in_missing_waypoints(s) })
          control.sources[i] = ETL::Control::EnumerableSource.new(control, configuration, s.definition)
        end
      end

      private
      def fill_in_missing_waypoints(source)
        rows = read(source)
        
        rows.each_with_index do |row, i|
          next unless row[:waypoint].to_s.empty?
          if i == 0 # first
            row[:waypoint] = row[:start_point]
          elsif i == rows.length - 1 # last
            row[:waypoint] = row[:end_point]
          else
            prev_unit, next_unit = rows[i-1], rows[i+1]
            if row[:reach_no] != prev_unit[:reach_no]
              row[:waypoint] = row[:start_point]
            elsif row[:Reach_No] != next_unit[:Reach_No]
              row[:waypoint] = row[:end_point]
            end
          end
        end
        
        rows
      end

      def read(source)
        rows = []
        source.each { |r| rows << r }
        rows
      end

      def write(source, rows)
        new_source = source.local_file
        FasterCSV.open(new_source, "w", { :headers => source.definition }) do |csv|
          rows.each { |row| csv << row }
        end
      end
    end
  end
end