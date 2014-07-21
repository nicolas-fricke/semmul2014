# encoding: utf-8

require 'yaml'
require 'csv'
require_relative '../lib/dbpedia_crawler'

DEFAULT_PATH = '../demo/movie_links.csv'

def parse_movie_file(path)
  p path
  movie_links = []
  CSV.foreach(path) do |name, dbpedia_link, _, _|
    movie_links << dbpedia_link
  end
  movie_links
end


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
      if ARGV.first
        DBpediaCrawler::Crawler.new(configuration).start_demo parse_movie_file(ARGV.first)
      elsif File.exists? DEFAULT_PATH
        DBpediaCrawler::Crawler.new(configuration).start_demo parse_movie_file(File.absolute_path DEFAULT_PATH)
      else
        p "please specify a cvs file"
      end
    rescue Exception => e
      puts "### STOP: Uncaptured error while running the crawler:"
      puts e.message, e.backtrace
      exit
    end
  end

end

# start the crawler
DBpediaCrawler::Application.new.run

