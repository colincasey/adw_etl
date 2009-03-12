module ETL #:nodoc:
  module Control #:nodoc:
    class CsvDestination < Destination
      attr_reader :file, :append, :headers, :order
      # Initialize the object.
      # * <tt>control</tt>: The Control object
      # * <tt>configuration</tt>: The configuration map
      # * <tt>mapping</tt>: The output mapping
      #
      # Configuration options:
      # * <tt>:file<tt>: The file to write to (REQUIRED)
      # * <tt>:append</tt>: Set to true to append to the file (default is to overwrite)
      # * <tt>:headers</tt>: Set to true to add the headers to the output also (default is true)
      #
      # Mapping options:
      # * <tt>:order</tt>: The order array
      def initialize(control, configuration, mapping={})
        super
        @file = File.join(File.dirname(control.file), configuration[:file])
        @append = configuration[:append] ||= false
        @headers = configuration[:headers] ||= true
        @order = mapping[:order] || order_from_source
        raise ControlError, "Order required in mapping" unless @order
      end

      # Close the destination. This will flush the buffer and close the underlying stream or connection.
      def close
        flush
        f.close
      end

      # Flush the destination buffer
      def flush
        if write_header?
          f << order
        end

        #puts "Flushing buffer (#{file}) with #{buffer.length} rows"
        buffer.flatten.each do |row|
          # check to see if this row's compound key constraint already exists
          # note that the compound key constraint may not utilize virtual fields
          next unless row_allowed?(row)
          # add any virtual fields
          add_virtuals!(row)
          # collect all of the values using the order designated in the configuration
          values = order.collect do |name|
            value = row[name]
            case value
            when Date, Time, DateTime
              value.to_s(:db)
            else
              value
#              value.to_s
            end
          end
          # write the values
          f << values
        end
        f.flush
        buffer.clear
      end

      private
      # Get the open file stream
      def f
        @f ||= FasterCSV.open(file, mode)
      end

      # Get the appropriate mode to open the file stream
      def mode
        append ? 'a' : 'w'
      end

      def write_header?
        if headers
          @headers = false
          return true
        end
        return false
      end

    end
  end
end
