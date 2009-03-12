require File.dirname(__FILE__) + '/../../test_helper'

class NullifyEmptyValuesProcessorTest < Test::Unit::TestCase
  should "nullify a blank value in a row" do
    row = process({ :a => '' })
    assert_nil row[:a]
  end

  should "not nullify a non-empty value in a row" do
    row = process({ :a => 'test' })
    assert_equal 'test', row[:a]
  end

  context "when the :only option is specified" do
    context "for multiple values" do
      setup do
        @row = process({ :a => '', :b => '', :c => '' }, { :only => [:a, :b] })
      end
      should("only nullify the values named") { assert_nil @row[:a]; assert_nil @row[:b] }
      should("not nullify the values that aren't named") { assert_equal '', @row[:c] }
    end

    context "for a single value" do
      setup do
        @row = process({ :a => '', :b => '' }, { :only => :a })
      end
      should("only nullify the value named") { assert_nil @row[:a] }
      should("not nullify a value that isn't named") { assert_equal '', @row[:b] }
    end
  end

  context "when the :except option is specified" do
    context "for multiple values" do
      setup do
        @row = process({ :a => '', :b => '', :c => '' }, { :except => [:b, :c] })
      end
      should("not nullify the values that are named") { assert_equal '', @row[:b]; assert_equal '', @row[:c] }
      should("nullify the values that aren't named") { assert_nil @row[:a] }
    end

    context "for a single value" do
      setup do
        @row = process({ :a => '', :b => '' }, { :except => :a })
      end
      should("not nullify the value specified") { assert_equal '', @row[:a] }
      should("nullify a value that isn't specified") { assert_nil @row[:b] }
    end
  end

  def process(row, configuration = {})
    control = mock
    p = ETL::Processor::NullifyEmptyValuesProcessor.new(control, configuration)
    p.process(row)
  end
end