# encoding: utf-8

# A crawler repeatedly receives commands from a queue and uses the fetcher 
# to execute them, thus crawling a DBpedia. The results are written to a
# triple store using a writer.
class DBpediaCrawler::Crawler

private

  #
  # commands
  #

  # Execute a given command
  #   command: hash
  def execute(command)
    type = command[:command]
    begin
      case type
      when :all_ids
        query_all_ids
      when :crawl_entity
        crawl_entity command
      end
    rescue StandardError => e
      puts "Execution of command failed: " + e.message
      retry_command command
    end
  end

  # Push the given command hash to the queue again if it may be retried.
  # Otherwise, discard it.
  def retry_command(command)
    retries = command[:retries].is_a?(Integer) ? command[:retries] : 0
    if retries > 0
      puts "Remaining retries: #{retries}. Pushing to the queue again."
      retries -= 1
      @queue.push command.merge({retries: retries})
    else
      puts "No retries. Discarding command."
    end
  end

  # Query all IDs of relevant entities. Add corresponding commands (fetch
  # linked data on these IDs) to the queue. Pagination is used for querying
  # but fetching commands are created after querying to achieve atomicity.
  def query_all_ids
    # get all movies
    puts "Fetching movies..."
    movies = @fetcher.query_movie_ids
    puts "Fetching shows..."
    shows = @fetcher.query_show_ids
    # create commands for fetching
    puts "Creating commands for fetching..."
    movies.each do |uri| 
      @queue.push(command: :crawl_entity, retries: @config["command_retries"], uri: uri, type: "movie")
    end
    shows.each do |uri| 
      @queue.push(command: :crawl_entity, retries: @config["command_retries"], uri: uri, type: "show") 
    end
  end

  # Query linked data on the given entity and write it to the data store.
  #   command: hash
  def crawl_entity(command)
    @fetcher.fetch(command[:uri], command[:type]) do |data|
      # one graph of data per (related) entity
      @writer.update(command[:uri], data)
    end
  end

public

  # Create a new Crawler using the given configuration hash.
  def initialize(configuration)
    # get configuration
    @config = configuration["crawler"]
    # create other components
    @queue = DBpediaCrawler::Queue.new(configuration["queue"], "crawler")
    @source = DBpediaCrawler::Source.new configuration["source"]
    @writer = DBpediaCrawler::Writer.new configuration["writer"]
    @fetcher = DBpediaCrawler::Fetcher.new(@source, @config["types"])
  end

  # Start the crawler. Pushes the initial command (to query all relevant
  # IDs) to the command queue and enters an inifinite loop which gets and
  # executes commands.
  def run
    # initial command: query all ids
    if @config["crawl_all_ids"] === true
      @queue.push(command: :all_ids, retries: @config["command_retries"])
    end
    # loop: get and execute commands
    loop do
      command = @queue.pop
      unless command.nil?
        puts "### Executing command: #{command}..."
        execute command
      else
        # no command available: sleep
        puts "Sleeping..."
        sleep @config["sleep_seconds"]
      end
    end
  end

end
