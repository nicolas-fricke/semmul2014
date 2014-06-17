# encoding: utf-8

require "linkeddata"
require_relative "queue"
require_relative "source"
require_relative "writer"

# A crawler crawls a DBpedia.
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
    :count_movies,  # query number of distinct movies
    :movies,        # query a page of movies
    :count_shows,   # query number of distinct shows
    :shows          # query a page of shows
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

  # Query all IDs of relevant entities. Add corresponding commands (fetch
  # linked data on these IDs) to the queue. Pagination is used for querying
  # but fetching commands are created after querying to achieve atomicity.
  def query_all_ids
    # get all movies
    puts "Fetching movies..."
    movies = @source.query_with_pagination(@queries[:movies], @queries[:count_movies])
    puts "Fetching shows..."
    shows = @source.query_with_pagination(@queries[:shows], @queries[:count_shows])
    # create commands for fetching
    puts "Creating commands for fetching..."
    movies.each { |uri| @queue.push(:crawl_entity, [uri, :movie]) }
    shows.each { |uri| @queue.push(:crawl_entity, [uri, :show]) }
  end

  # Query linked data on the given entity and write it to the data store.
  def crawl_entity(uri)
    @writer.insert @source.triples_for(uri) if uri
  end

  # Execute a given command
  #   command: { command: symbol, params: array }
  def execute(command)
    type = command[:command]
    params = command[:params]
    begin
      case type
      when :all_ids
        query_all_ids
      when :crawl_entity
        crawl_entity params[0]
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
    # get configuration
    @config = configuration["crawler"]
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
    # initial command: query all ids
    @queue.push :all_ids if @config["crawl_all_ids"] === true
    # loop: get and execute commands
    loop do
      command = @queue.pop
      unless command.nil?
        puts "Executing command: " + command.to_s + "..."
        execute command
      else
        # no command available: sleep
        puts "Sleeping..."
        sleep @config["sleep_seconds"]
      end
    end
  end

end
