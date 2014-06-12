# encoding: utf-8

require 'yaml'
require_relative '../lib/dbpedia_crawler/crawler'

# Command line application:
#
# Read the configuration file, merge the resulting hash with any command line
# arguments, and create a crawler using these options.
#
# Structure of command-line arguments:
#   -component:option:value
# where value can be a boolean (true, false), an integer, or a string (otherwise).
# So, to overwrite the "endpoint" of the "source" component, the argument is:
#   -source:endpoint:"http://live.dbpedia.org/sparql"
# Arguments should overwrite options from the configuration file. Other
# arguments will not be filtered (they are just irrelevant to the application).

def parse_arguments
  args = {}
  ARGV.each do |arg|
    if arg =~ /\A-(\w+):(\w+):(.+)/
      component, option, value = $1, $2, $3
      args[component] = {} if args[component].nil?
      if value =~ /\A\d+\Z/ # integer?
        args[component][option] = value.to_i
      elsif value =~ /\A((true)|(false))\Z/ # boolean?
        args[component][option] = value =~ /\Atrue\Z/ ? true : false
      else # string!
        args[component][option] = value
      end
    end
  end
  return args
end

begin
  # get configuration
  configuration = YAML.load_file File::expand_path("../configuration.yml", __FILE__)
  arguments = parse_arguments
  configuration.each_key { |key| configuration[key].merge! (arguments[key] || {}) }
rescue StandardError => e
  puts "# STOP: Error while retrieving the configuration:"
  puts e.message, e.backtrace
end

begin
  # start crawler
  DBpediaCrawler::Crawler.new(configuration).run
rescue Exception => e
  puts "# STOP: Uncaptured error while running the crawler:"
  puts e.message, e.backtrace
end
