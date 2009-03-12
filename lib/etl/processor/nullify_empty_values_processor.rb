module ETL #:nodoc:
  module Processor #:nodoc:
    # Row processor that converts all values that meet the requirement value.to_s.empty?
    # into nil unless :only or :except option is specified
    class NullifyEmptyValuesProcessor < RowProcessor
      attr_reader :only, :except
      # Initialize the processor
      # Configuration options:
      # * <tt>:only</tt>: A single key or an array of keys to be processed
      # * <tt>:except</tt>: A single key or an array of keys to omit from the process
      def initialize(control, configuration)
        super
        @only = configuration[:only]
        @except = configuration[:except]
        raise 'Specify either :only or :except, not both' if @only && @except
      end

      def process(row)
        if only
          keys = [*only]
        elsif except
          keys = row.keys - [*except]
        else
          keys = row.keys
        end
        keys.each { |key| row[key] = nil if row[key].to_s.blank? }
        row
      end      
    end
  end
end