# encoding: utf-8

require "bunny"
require "yaml"

# Abstracts from the underlying queue mechanism (Bunny)
# and allows handling of commands for the crawler.
class DBpediaCrawler::Queue

  # Create a new Queue
  #   configuration: hash
  def initialize(configuration)
    @agent_id = configuration["agent_id"]
    @queue = Bunny.new.start.create_channel.queue("semmul." + @agent_id, durable: true)
    @queue.purge if configuration["purge"] === true
  end

  # Get the next command, converted to a hash.
  #   result: { command: symbol, params: array } or nil (no message)
  def pop
    begin
      yaml_string = @queue.pop[2] # delivery info, message properties, message content
      return yaml_string != nil ? YAML.load(yaml_string) : nil
    rescue StandardError => e
      puts "# Error while popping command from queue:"
      puts e.message, e.backtrace
      return nil
    end
  end

  # Create a command and push it to the queue. The command hash is
  # stringified using yaml (which can be parsed into a hash again without
  # using the evil "eval").
  #   command: symbol (e.g. :crawl_ids)
  #   params: array of strings (may be empty)
  def push(command, params = [])
    hash = {command: command, params: params}
    begin
      @queue.publish(hash.to_yaml, persistent: true)
    rescue StandardError => e
      puts "# Error while publishing command to queue:"
      p hash
      puts e.message, e.backtrace
    end
  end

end
