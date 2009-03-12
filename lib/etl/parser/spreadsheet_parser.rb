module ETL #:nodoc:
  module Parser #:nodoc:
    # Parses spreadsheet files
    class SpreadsheetParser < ETL::Parser::Parser
      # Initialize the parser
      # * <tt>source</tt>: The Source object
      #
      # Configuration options:
      # * <tt>:type</tt>: Either :excel, :excel_x, :open_office, or :google      
      # * <tt>:sheet</tt>: The name or index of the sheet to read from
      # * <tt>:first_row</tt>:
      # * <tt>:last_row</tt>:
      # * <tt>:first_column</tt>:
      # * <tt>:last_column</tt>: 
      def initialize(source, options={})
        super
        raise ':sheet is a required option' unless options[:sheet]
        @sheet = options[:sheet]
        @first_row = options[:first_row]
        @last_row = options[:last_row]
        @first_column = options[:first_column]
        @last_column = options[:last_column]
        configure
      end
      
      # Returns each row.
      def each
        Dir.glob(file).each do |file|
          ETL::Engine.logger.debug "parsing #{file}"
          spreadsheet = create_spreadsheet(file)
          spreadsheet.default_sheet = @sheet
          first_row = @first_row || spreadsheet.first_row
          last_row  = @last_row  || spreadsheet.last_row
          first_column = @first_column || spreadsheet.first_column
          last_column = @last_column || spreadsheet.last_column
          #puts "#({first_row},#{first_column}):(#{last_row},#{last_column})"

          current_row = first_row
          while(current_row <= last_row) do
            raw_row = []
            current_column = first_column
            while(current_column <= last_column) do
              raw_row << spreadsheet.cell(current_row, current_column)
              current_column += 1
            end
            current_row += 1
            #puts "#{raw_row.inspect}"
            row = {}
            validate_row(raw_row, current_row, file)
            raw_row.each_with_index do |value, index|
              f = fields[index]
              row[f.name] = value
            end
            yield row
          end
        end
      end
      
      # Get an array of defined fields
      def fields
        @fields ||= []
      end
      
      private
      def validate_row(row, line, file)
        ETL::Engine.logger.debug "validating line #{line} in file #{file}"
        if row.length != fields.length
          raise_with_info( MismatchError, 
            "The number of columns from the source (#{row.length}) does not match the number of columns in the definition (#{fields.length})", 
            line, file
          )
        end
      end
      
      def configure
        source.definition.each do |options|
          case options
          when Symbol
            fields << Field.new(options)
          when Hash
            fields << Field.new(options[:name])
          else
            raise DefinitionError, "Each field definition must either be a symbol or a hash"
          end
        end
      end

      def create_spreadsheet(file)
        case options[:type]
        when :excel
          Excel.new(file)
        when :excel_x
          Excelx.new(file)
        when :open_office
          OpenOffice.new(file)
        when :google
          Google.new(file)
        else
          raise 'Spreadsheet parser must be of type :excel, :excel_x, :open_office, or :google'
        end
      end
      
      class Field #:nodoc:
        attr_reader :name
        def initialize(name)
          @name = name
        end
      end
    end
  end
end