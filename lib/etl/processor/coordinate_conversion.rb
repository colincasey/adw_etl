module ETL #:nodoc:
  module Processor #:nodoc:
    class CoordinateConversionProcessor < ETL::Processor::Processor
      def process(row)
        return row if row[:waypoint].to_s.empty?
        y, x = row[:waypoint].split(',')
        point = Point.new(x, y)
        row[:lat] = point.y
        row[:lon] = point.x
        row
      end

      class Point
        DEGREES_MINUTES_SECONDS_FORMAT = /^(-?\d{2}\d?)[:d° ](\d\d?)[:' ](\d\d?[.]?\d*)[" ]?([NSEW]?)$/
        DEGREES_DECIMAL_MINUTES_FORMAT = /^(-?\d{2}\d?)[:d° ](\d+[.]?\d*)[' ]?([NSEW]?)$/
        DECIMAL_DEGREES_FORMAT = /^(-?\d+[.]?\d*)[ ]?([NSEW]?)$/

        attr_reader :x, :y

        def initialize(x, y)
          @x, @y = x, y # set this here to get info full point info during parse routine
          @x, @y = parse(x), parse(y)
        end

        private
        def parse(coord)
          if decimal_degrees?(coord)
            parse_decimal_degrees(coord)
          elsif degrees_decimal_minutes?(coord)
            parse_degrees_decimal_minutes(coord)
          elsif degrees_minutes_seconds?(coord)
            parse_degrees_minutes_seconds(coord)
          else
            raise "Point coordinate #{coord} is not in a known format for point(#{@x}, #{@y})"
          end
        end

        def decimal_degrees?(coord)
          coord.to_s.match(DECIMAL_DEGREES_FORMAT)
        end

        def parse_decimal_degrees(coord)
          coord.to_s.match(DECIMAL_DEGREES_FORMAT)
          decimal_degrees, direction = $1.to_f, $2
          if direction == 'S' || direction == 'W'
            decimal_degrees = -decimal_degrees
          end
          decimal_degrees
        end

        def degrees_minutes_seconds?(coord)
          coord.to_s.match(DEGREES_MINUTES_SECONDS_FORMAT)
        end

        def parse_degrees_minutes_seconds(coord)
          coord.to_s.match(DEGREES_MINUTES_SECONDS_FORMAT)
          degrees, minutes, seconds, direction = $1.to_f, $2.to_f, $3.to_f, $4
          raise "Value #{coord} can't have both a negative degree value and a NSEW specifier" if degrees < 0 && direction.to_s.empty?
          decimal_degrees = degrees.abs + (minutes  * 60.0 + seconds) / 3600.0
          if degrees < 0 || direction == 'S' || direction == 'W'
            decimal_degrees = -decimal_degrees
          end
          decimal_degrees
        end

        def degrees_decimal_minutes?(coord)
          # possible formats:
          #   40°26.7,  -79°56.9
          #   40d26.7m, -79d56.9'
          #   40 26.7N,  -79 56.9
          coord.to_s.match(DEGREES_DECIMAL_MINUTES_FORMAT)
        end

        def parse_degrees_decimal_minutes(coord)
          coord.to_s.match(DEGREES_DECIMAL_MINUTES_FORMAT)
          degrees, decimal_minutes, direction = $1.to_f, $2.to_f, $3
          raise "Value #{coord} can't have both a negative degree value and a NSEW specifier" if degrees < 0 && direction.to_s.empty?
          decimal_degrees = degrees.abs + (decimal_minutes / 60)
          if degrees < 0 || direction == 'S' || direction == 'W'
            decimal_degrees = -decimal_degrees
          end
          decimal_degrees
        end
      end
    end
  end
end