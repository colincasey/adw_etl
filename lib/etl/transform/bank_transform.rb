module ETL #:nodoc:
  module Transform #:nodoc:    
    class BankTransform < Transform
      LEFT_BANK  = 'L'
      RIGHT_BANK = 'R'

      def transform(name, value, row)
        value = value.to_s
        if value.match(/^(L|left)$/i)
          LEFT_BANK
        elsif value.match(/^(R|right)$/i)
          RIGHT_BANK
        else
          nil
        end
      end
    end
  end
end