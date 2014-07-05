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
# Arguments should overwrite options for components from the configuration file. 
# Other arguments are irrelevant.
class DBpediaCrawler::Application

private

  # path to the YAML file containing the default configuration
  # as well as types and fetching rules
  CONFIG_FILE = "../../../config/dbpedia.yml"

  # path to the YAML file containing the global configuration
  GLOBAL_CONFIG_FILE = "../../../config/namespaces.yml"

  def parse_arguments
    args = {}
    ARGV.each do |arg|
      if arg =~ /\A-(\w+):(\w+):(.+)/
        component, option, value = $1, $2, $3
        args[component] = {} if args[component].nil?
        if value =~ /\A\d+\Z/                  # integer?
          args[component][option] = value.to_i
        elsif value =~ /\A((true)|(false))\Z/  # boolean?
          args[component][option] = value =~ /\Atrue\Z/ ? true : false
        else                                   # string!
          args[component][option] = value
        end
      end
    end
    return args
  end

  def load_configuration
    YAML.load_file File::expand_path(CONFIG_FILE, __FILE__)
  end

  def load_global_configuration
    global_config = YAML.load_file File::expand_path(GLOBAL_CONFIG_FILE, __FILE__)

    # get mapper queue, remove trailing "."
    mapper_queue = global_config["queues"]["raw_dbpedia"]
    if mapper_queue.end_with? "."
      mapper_queue = mapper_queue[0..-2]
    end

    # construct and return hash for merging
    result = {}
    result["crawler"] = { "mapper_queue" => mapper_queue }
    return result
  end

public

  # Execute the application
  def run
    begin
      # parse default / global / command-line options
      configuration = load_configuration
      global_configuration = load_global_configuration
      arguments = parse_arguments
      # merge options (precedence: command_line > global > default)
      configuration.each_key { |key| configuration[key].merge! (global_configuration[key] || {}) }
      configuration.each_key { |key| configuration[key].merge! (arguments[key] || {}) }
      # start crawler
      DBpediaCrawler::Crawler.new(configuration).run
    rescue Exception => e
      puts "### STOP: Uncaptured error while running the crawler:"
      puts e.message, e.backtrace
      exit
    end
  end

end

# start the crawler
DBpediaCrawler::Application.new.run

