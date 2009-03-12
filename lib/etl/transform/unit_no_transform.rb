module ETL #:nodoc:
  module Transform #:nodoc:
    class UnitNoTransform < Transform
      def transform(name, value, row)
        case value
        when Integer
          transform_integer(value)
        when String
          transform_string(value)
        when Float
          transform_float(value)
        else
          nil
        end
      end
      
      private
      def transform_integer(value)
        if value >= 1
          pad(value.to_s)
        else
          #log.warning("Invalid integer value for Unit No. Transform: #{value} < 1")
        end
      end

      def transform_string(value)
        if value.to_i < 1
          #log.warning("Invalid string value for Unit No. Transform: #{value}")
        else
          transform_integer(value.to_i)
        end
      end

      def transform_float(value)
        int, frac = value.to_s.split('.')
        if frac.to_i > 0 || int.to_i < 1
          #log.warning("Invalid float value for Unit No. Transform: #{value}")
        else
          transform_integer(int.to_i)
        end
      end

      def pad(str)
        str.rjust(3, "0")
        
      end
    end
  end
end