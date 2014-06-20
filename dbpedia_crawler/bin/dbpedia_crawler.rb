# encoding: utf-8

require 'yaml'
require_relative '../lib/dbpedia_crawler'

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
# arguments are irrelevant.
class DBpediaCrawler::Application

private

  # path to the YAML file containing the default configuration
  DEFAULT_CONFIG_FILE = "../../lib/dbpedia_crawler/configuration/options.yml"

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

public

  # Execute the application
  def run
    begin
      # parse options
      configuration = YAML.load_file File::expand_path(DEFAULT_CONFIG_FILE, __FILE__)
      arguments = parse_arguments
      configuration.each_key { |key| configuration[key].merge! (arguments[key] || {}) }
      # start crawler
      DBpediaCrawler::Crawler.new(configuration).run
    rescue Exception => e
      puts "# STOP: Uncaptured error while running the crawler:"
      puts e.message, e.backtrace
      exit
    end
  end

end

# start the crawler
DBpediaCrawler::Application.new.run

