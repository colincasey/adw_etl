module ETL #:nodoc:
  module Processor #:nodoc:
    class CalculateUnitLengthsProcessor < ETL::Processor::Processor
      def initialize(control, configuration)
        super
        @calibrate = configuration[:calibrate]
      end

      def process
        control.sources.each_with_index do |s, i|
          control.sources[i] = ETL::Control::EnumerableSource.new(control, { :enumerable => calculate_unit_lengths(s) }, s.definition)
        end
      end

      private
      def calibrate?
        !!@calibrate
      end

      def calculate_unit_lengths(source)
        rows = []
        sections = sections(source)
        sections.each_with_index do |section, i|
          start = i==0 ? source.configuration[:start_route_measure] : sections[i-1].units.last[:to_measure]
          rows << calculate_unit_lengths_for_section(section, start).units
        end
        rows.flatten
      end

      def calculate_unit_lengths_for_section(section, start_measure)
        section.units.each_with_index do |unit, i|
          if i == 0
            from = start_measure
          else
            previous = section.units[i-1]
            from = previous[:to_measure]
          end

          case unit[:channel_type]
          when 1
            to = from - unit[:unit_length].to_f
          when 2
            to = previous[:to_measure]
          when 3
            raise 'channel 3 not implemented'
          when 4
            to = previous[:to_measure]
          end

          unit[:from_measure] = from
          unit[:to_measure] = to
        end

        # corrent type 4 units on second pass
        section.units.each do |unit|
          if unit[:channel_type] == 4
            unit[:to_measure] = unit[:to_measure] + unit[:unit_length]
          end
        end

        section
      end

      def sections(source)
        rows = []
        source.each { |r| rows << r }
        
        sections = [Section.new]
        rows.each_with_index do |row, i|
          previous_route_no = i == 0 ? nil : rows[i-1][:reach_no]
          if previous_route_no && previous_route_no != row[:reach_no]
            sections << Section.new
          end
          current_section = sections.last
          current_section << row
        end

        sections
      end

      class Section
        attr_accessor :units

        def initialize
          @units = []
        end

        def <<(row)
          units << row
        end

        def to_s
          "section contains #{units.length} units measuring #{units.first[:from_measure]}-#{units.last[:to_measure]}"
        end
      end
    end
  end
end