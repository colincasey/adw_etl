module ETL #:nodoc:
  module Transform #:nodoc:
    class CoordinateTransform < ETL::Transform::Transform
      attr_accessor :as

      # Initialize the transformer
      #
      # Configuration options:
      # * <tt>:as</tt>: The field to save the standardized value as (defaults to the name of the target field)
      def initialize(control, name, configuration={})
        super
        @as = configuration[:as]
      end
      
      # Transform the value
      def transform(name, value, row)
        return if value.to_s.empty?
        original_value = value

        value = value.gsub(/(WO|W0)/, 'W')
        value = value.gsub('/', '')

        value = value.strip
        split_at = %w(N S E W).collect{ |dir| value.index(dir).to_i }.max
        coord_pair = [
          value[0..(split_at - 1)].strip,
          value[split_at..-1].strip
        ]
        coord_pair.collect! do |coord|
          coord = (coord.gsub(/(N|S|E|W)/, '') + $1).strip
        end
        x = coord_pair[0].match(/E|W/) ? coord_pair[0] : coord_pair[1]
        y = coord_pair[0].match(/N|S/) ? coord_pair[0] : coord_pair[1]

        #raise "cleanup point data routine tried to set both x and y to the same value #{point}" if x == y
        clean_format = "#{y},#{x}"
        #log.debug "cleaning up point data for \"#{point}\" yielded \"#{clean_format}\""
        if as
          row[as] = clean_format
          original_value
        else
          clean_format
        end
      end
    end
  end
end