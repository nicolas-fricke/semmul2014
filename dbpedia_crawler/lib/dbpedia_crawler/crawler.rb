# encoding: utf-8

require "linkeddata"
require_relative "queue"
require_relative "source"
require_relative "writer"

# A DBpediaCrawler is a crawler which crawls a DBpedia. :)
class DBpediaCrawler::Crawler

private

  #
  # SPARQL queries
  #

  # path of the folder with the queries, ending with "/"
  QUERIES_PATH = File::expand_path("../queries", __FILE__) + "/"
  # file extension of files with queries
  QUERIES_FILE_EXT = ".txt"

  # symbols which denote queries
  QUERIES = [
    # query the IRIs of relevant entities
    :all_ids,
    :count_ids,
    :ids_limit_offset
  ]

  # load query strings from files
  def initialize_queries
    @queries = {}
    QUERIES.each do |symbol|
      File.open(QUERIES_PATH + symbol.to_s + QUERIES_FILE_EXT, "r") do |file|
        @queries[symbol] = file.read
      end
    end
  end

  #
  # commands
  #

  # Query all IDs of relevant entities.
  #   result: array of strings
  def query_all_ids
    @source.query(@queries[:all_ids]).map do |solution|
      solution[:movie].to_s
    end
  end

  # Execute a given command
  #   command: { command: symbol, params: array }
  def execute(command)
    type = command[:command]
    params = command[:params]
    begin
      if type == :all_ids
        # query all IDs and add corresponding commands to the queue
        query_all_ids.each do |uri|
          @queue.push(:crawl_entity, [uri])
        end
      elsif type == :crawl_entity
        uri = params[0]
        @writer.insert @source.triples_for(uri) if uri
      end
    rescue StandardError => e
      puts "Execution of command failed: " + e.message
      puts "Pushing command to the queue again."
      @queue.push(type, params)
    end
  end

public

  #
  # instance creation
  #

  # Create a new Crawler using the given configuration hash.
  def initialize(configuration)
    @sleep_time = configuration["crawler"]["sleep_seconds"]
    # create other components with the given configuration
    @queue = DBpediaCrawler::Queue.new configuration["queue"]
    @source = DBpediaCrawler::Source.new configuration["source"]
    @writer = DBpediaCrawler::Writer.new configuration["writer"]
    # load query strings
    initialize_queries
  end

  # Start the crawler. Pushes the initial command (to query all relevant
  # IDs) to the command queue and enters an inifinite loop which gets and
  # executes commands.
  def run
    # command: query all ids
    @queue.push :all_ids
    # loop: get and execute commands
    loop do
      command = @queue.pop
      unless command == nil
        puts "Executing command: " + command.to_s + "..."
        execute command
      else
        # no command available: sleep 5 seconds
        puts "Sleeping..."
        sleep @sleep_time
      end
    end
  end

end
