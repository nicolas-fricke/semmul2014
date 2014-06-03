require "bunny"
require "yaml"

module DBpediaCrawler

  # Abstracts from the underlying queue mechanism (Bunny)
  # and allows handling of commands for the crawler.
  # TODO: check bunny options
  # TODO: convention regarding queue names
  class Queue

    # Create a new Queue
    #   configuration: hash
    def initialize(configuration)
      @agent_id = configuration["agent_id"]
      @queue = Bunny.new.start.create_channel.queue("semmul." + @agent_id, durable: true)
      @queue.purge # TODO: remove that later!
    end

    # Get the next command, converted to a hash.
    #   result: { command: symbol, params: array } or nil (no message)
    def pop
      yaml = @queue.pop[2] # delivery info, message properties, message content
      return yaml != nil ? YAML.load(yaml) : nil
    end

    # Create a command and push it to the queue. The command hash is
    # stringified using yaml (which can be parsed into a hash again without
    # using the evil "eval").
    #   command: symbol (e.g. :crawl_ids)
    #   params: array of strings (may be empty)
    def push(command, params = [])
      hash = {command: command, params: params}
      @queue.publish(hash.to_yaml, persistent: true)
    end

  end

end
