# encoding: utf-8

# A crawler repeatedly receives commands from a queue and uses the fetcher 
# to execute them, thus crawling a DBpedia. The results are written to a
# triple store using a writer.
class DBpediaCrawler::Crawler

private

  # DBpedia ontology type for movies
  DB_MOVIE = "http://dbpedia.org/ontology/Film"

  # DBpedia ontology type for shows
  DB_SHOW = "http://dbpedia.org/ontology/TelevisionShow"

  #
  # initialization
  #

  # path to the YAML file containing the types and rules for fetching
  TYPES_FILE = "../configuration/types.yml"

  # Load the types and the fetching rules.
  #   result: [types_hash, rules_hash]
  def load_types
    file_hash = YAML.load_file File::expand_path(TYPES_FILE, __FILE__)
    return [file_hash["types"], file_hash["rules"]]
  end
  
  # Create all queues that the crawler will listen to.
  # This includes a "crawler" queue for the "crawl all IDs" command
  # as well as dedicated queues for each entity type (although the
  # crawler itself will only push commands to "movie" and "show",
  # other components may push commands for "actor" etc).
  #   types: hash
  #   queue_config: hash
  #   result: hash
  def create_queues(types, queue_config)
    queues = {}

    # create queue for crawler
    queues["crawler"] = DBpediaCrawler::Queue.new(queue_config, "crawler")
    # create queues for types
    types.keys.each do |type|
      queues[type] = DBpediaCrawler::Queue.new(queue_config, type)
    end

    return queues
  end

  #
  # commands
  #

  # Pop the next command from one of the queues.
  # Use another queue each time.
  #   result: hash, string (query name)
  def next_command
    # try each queue, if necessary (all queues may be empty)
    @queues.keys.size.times do
      # pop a command (may be nil)
      type = @queues.keys[@queue_index]
      command = @queues[type].pop
      # increment queue index
      @queue_index = (@queue_index < (@queues.keys.size - 1)) ? (@queue_index + 1) : 0
      # return converted command or continue
      unless command.nil?
        command = convert_command(command, type)
        return [command, type]
      end
    end
    # all queues are empty
    return [nil, nil]
  end

  # If the given command is just a string or if it is missing some infos,
  # create a proper command hash. This is necessary because the crawler uses
  # hashes as commands, but other agents may just push URIs to the respective
  # queues (meaning "fetch the given entity").
  #   command: object fetched from a queue
  #   type: string (queue name)
  #   result: hash
  def convert_command(command, type)
    # commands from the "crawler" queue should be correct
    return command if type == "crawler"

    # convert to hash, if necessary
    unless command.is_a? Hash
      # assume that the command is a URI
      command = { uri: command.to_s }
    end
    # guess missing parameters
    unless command.has_key?(:command) and not command[:command].nil?
      # no command: assume that the entity shall be fetched
      command[:command] = :crawl_entity
    end
    unless command.has_key?(:type) and not command[:type].nil?
      # no type: use queue name as type
      command[:type] = type
    end

    return command
  end

  # Execute a given command
  #   command: hash
  #   queue_name: string
  def execute(command, queue_name)
    action = command[:command]
    begin
      case action
      when :all_ids
        query_all_ids
      when :crawl_entity
        crawl_entity command
      end
    rescue StandardError => e
      puts "Execution of command failed: " + e.message
      retry_command(command, queue_name)
    end
  end

  # Push the given command hash to the queue again if it may be retried.
  # Otherwise, discard it.
  def retry_command(command, queue_name)
    # get retries (no retries specified: assume default retries)
    retries = command[:retries].is_a?(Integer) ? command[:retries] : @config["command_retries"]
    if retries > 0
      # retries left: push again
      puts "Remaining retries: #{retries}. Pushing to the queue again."
      retries -= 1
      @queues[queue_name].push command.merge({retries: retries})
    else
      # no retries left: discard
      puts "No retries. Discarding command."
    end
  end

  # Query all IDs of relevant entities. Add corresponding commands (fetch
  # linked data on these IDs) to the queue. Pagination is used for querying.
  # Depending on the options, queried entities are validated using type
  # inference.
  def query_all_ids
    query_all_movies
    query_all_shows
  end

  # Query all IDs of movies.
  def query_all_movies
    puts "Fetching movies..."
    @fetcher.query_movie_ids do |movies|
      puts "Creating commands for fetching..."
      movies.each do |uri| 
        # check types, if specified
        if @config["check_types"]
          begin
            unless @type_checker.entity_has_type?(uri, DB_MOVIE)
              puts "  #{uri} is discarded (probably not a movie)"
              next
            else
              puts "  #{uri} is pushed to the queue"
            end
          rescue StandardError => e
            puts "  type checking failed" # message is too long for printing
            puts "    #{uri} is pushed to the queue"
          end
        end
        # add command to the queue
        @queues["movie"].push(command: :crawl_entity, retries: @config["command_retries"], uri: uri, type: "movie")
      end
    end
  end

  # Query all IDs of shows.
  def query_all_shows
    puts "Fetching shows..."
    @fetcher.query_show_ids do |shows|
      puts "Creating commands for fetching..."
      shows.each do |uri| 
        # check types, if specified
        if @config["check_types"]
          begin
            unless @type_checker.entity_has_type?(uri, DB_SHOW)
              puts "  #{uri} is discarded (probably not a show)"
              next
            else
              puts "  #{uri} is pushed to the queue"
            end
          rescue StandardError => e
            puts "  type checking failed" # message is too long for printing
            puts "    #{uri} is pushed to the queue"
          end
        end
        # add command to the queue
        @queues["show"].push(command: :crawl_entity, retries: @config["command_retries"], uri: uri, type: "show") 
      end
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
    # load types and fetching rules
    types, rules = load_types
    # create other components
    @queues = create_queues(types, configuration["queue"])
    @queue_index = 0	# index of the next queue to pop a command from
    @source = DBpediaCrawler::Source.new configuration["source"]
    @type_checker = DBpediaCrawler::TypeChecker.new configuration["type_checker"]
    @writer = DBpediaCrawler::Writer.new configuration["writer"]
    @fetcher = DBpediaCrawler::Fetcher.new(@source, types, rules)
  end

  # Start the crawler. Pushes the initial command (to query all relevant
  # IDs) to the command queue and enters an inifinite loop which gets and
  # executes commands.
  def run
    # initial command: query all ids
    if @config["crawl_all_ids"] === true
      @queues["crawler"].push(command: :all_ids, retries: @config["command_retries"])
    end
    # loop: get and execute commands
    loop do
      command, queue_name = next_command
      unless command.nil?
        puts "### Executing command: #{command}..."
        execute(command, queue_name)
      else
        # no command available
        if @config["insomnia"] === true
          # push the initial command again, thus starting another 
          # cycle of crawling and fetching
          puts "### Empty queue. Pushing initial command again..."
          @queues["crawler"].push(command: :all_ids, retries: @config["command_retries"])
        else
          # sleep
          puts "Sleeping..."
          sleep @config["sleep_seconds"]
        end
      end
    end
  end

end
