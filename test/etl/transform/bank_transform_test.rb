require File.dirname(__FILE__) + '/../../test_helper'

class BankTransformTest < Test::Unit::TestCase
  LEFT_BANK  = ETL::Transform::BankTransform::LEFT_BANK
  RIGHT_BANK = ETL::Transform::BankTransform::RIGHT_BANK

  should "return nil with any value not matching 'L', 'R', 'Left', or 'Right'" do
    invalid = [nil, 1, '', 'Louie', 'Ralphie', 'jaR', 'jaiL']
    invalid.each do |value|
      assert_nil transform(value), "#{value} is an invalid bank value"
    end
  end

  should "return '#{LEFT_BANK}' with case-insensitive variations of the word 'LEFT' or 'L'" do
    %w(LEFT Left left L l).each do |value|
      assert_equal LEFT_BANK, transform(value)
    end
  end

  should "return '#{RIGHT_BANK}' with case-insensitive variations of the word 'RIGHT' or 'R'" do
    %w(RIGHT Right right R r).each do |value|
      assert_equal RIGHT_BANK, transform(value)
    end
  end

  def transform(value, row = {})
    control = mock
    name = :bank
    t = ETL::Transform::BankTransform.new(control, name)
    t.transform(name, value, row)
  end
end