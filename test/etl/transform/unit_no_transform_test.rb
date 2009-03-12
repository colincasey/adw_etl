require File.dirname(__FILE__) + '/../../test_helper'

class UnitNoTransformTest < Test::Unit::TestCase
  should "return nil when value to transform is nil" do
    assert_nil transform(nil)
  end

  context "when transforming integer values" do
    should "pad value with two leading zeros if value > 0" do
      assert_equal "001", transform(1)
      assert_equal "010", transform(10)
      assert_equal "100", transform(100)
    end

    should "return nil if value <= 0" do
      assert_nil transform(0)
      assert_nil transform(-1)
    end
  end

  context "when transforming string values" do
    should "return nil when value contains only text" do
      assert_nil transform("test")
    end

    should "return nil when value is a blank string" do
      assert_nil transform('')
    end

    # because sometimes agencies add the bank indicator to the unit no
    should "pad value with two leading zeros if value is alphanumeric and can be converted into an integer" do
      assert_equal "001", transform("1L")
      assert_equal "010", transform("10R")
      assert_equal "100", transform("100L")
    end

    should "return nil if value is alphanumeric and cannot be converted into an integer" do
      assert_nil transform('L1')
    end
  end

  context "when transforming decimal values" do
    should "return nil when value < 1" do
      assert_nil transform(0.999)
      assert_nil transform(0.0)
      assert_nil transform(-1.0)
    end

    should "return nil when value has non-zero fractional part" do
      assert_nil transform(1.1)
    end

    should "pad value with two leading zeros if value converts to integer with no loss of precision" do
      assert_equal "001", transform(1.0)
      assert_equal "010", transform(10.0)
      assert_equal "100", transform(100.0)
    end
  end
  
  def transform(value, row = {})
    control = stub
    name = :unit_no
    t = ETL::Transform::UnitNoTransform.new(control, name)
    t.transform(name, value, row)
  end
end