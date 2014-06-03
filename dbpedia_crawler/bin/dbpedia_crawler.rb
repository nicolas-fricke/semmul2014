require 'yaml'
require_relative '../lib/dbpedia_crawler/crawler'

# Command line application:
#
# Read the configuration file, merge the resulting hash with any command line
# arguments, and create a crawler using these options.

# Structure of command-line arguments:
#   -component:option:value
# where value will be treated as string unless it only contains digit characters,
# in which case it will be converted to an integer.
# So, to overwrite the "endpoint" of the "source" component, the argument is:
#   -source:endpoint:"http://live.dbpedia.org/sparql"
# Arguments should overwrite options from the configuration file. Other
# arguments will not be filtered (they are just irrelevant to the application).
def parse_arguments
  args = {}
  ARGV.each do |arg|
    if arg =~ /\A-(\w+):(\w+):(.+)/
      component, option, value = $1, $2, $3
      args[component] = {} unless args[component]
      args[component][option] = value =~ /\A\d+\Z/ ? value.to_i : value
    end
  end
  return args
end

configuration = YAML.load_file File::expand_path("../configuration.yml", __FILE__)
configuration.merge! parse_arguments

DBpediaCrawler::Crawler.new(configuration).run
