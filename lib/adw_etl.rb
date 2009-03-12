gem 'activewarehouse-etl', ">= 0"
require 'etl'
gem 'sequel', ">= 2.8.0"
require 'sequel'
gem 'roo', ">= 1.2.3"
require 'roo'

Dir[File.dirname(__FILE__) + "/etl/**/*.rb"].each do |f|
  require f
end