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
    :all_ids,          # query all relevant IDs
    :count_ids,        # query number of relevant IDs
    :ids_limit_offset  # query a specific part of relevant IDs
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

  # Helper for printing a message like
  # "Querying results 10000 - 19999...",
  # while handling a paginated query.
  def print_query_page_message(count, limit, offset)
    first_string = offset.to_s.rjust(count.to_s.length)
    last_string = [count, offset + limit - 1].min.to_s.rjust(count.to_s.length)
    puts "Querying results " + first_string + " to " + last_string + "..."
  end

  # Query all IDs of relevant entities. Add corresponding commands (fetch
  # linked data on these IDs) to the queue. Pagination is used.
  def query_all_ids
    # get the number of URIs
    count = @source.query(@queries[:count_ids])[0][:result].to_s.to_i
    puts "Number of URIs: " + count.to_s
    # query with pages
    result = []
    (0..(count / @page_size).floor).each do |page_number|
      # get query string and apply parameters
      query = @queries[:ids_limit_offset].clone
      query["<<limit>>"]= @page_size.to_s
      query["<<offset>>"]= (page_number * @page_size).to_s
      # query and append results
      print_query_page_message(count, @page_size, page_number * @page_size)
      result.concat(@source.query(query).map { |solution| solution[:movie].to_s })
    end
    # create commands for fetching
    puts "Creating commands for fetching..."
    result.each { |uri| @queue.push(:crawl_entity, [uri]) }
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
      if type == :all_ids
        query_all_ids
      elsif type == :crawl_entity
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
    # get configuration values
    config = configuration["crawler"]
    @crawl_all_ids = config["crawl_all_ids"]
    @page_size = config["page_size"]
    @sleep_time = config["sleep_seconds"]
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
    @queue.push :all_ids if @crawl_all_ids
    # loop: get and execute commands
    loop do
      command = @queue.pop
      unless command.nil?
        puts "Executing command: " + command.to_s + "..."
        execute command
      else
        # no command available: sleep
        puts "Sleeping..."
        sleep @sleep_time
      end
    end
  end

end
