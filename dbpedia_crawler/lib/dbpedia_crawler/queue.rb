# encoding: utf-8

require "bunny"
require "yaml"

# Abstracts from the underlying queue mechanism (Bunny)
# and allows handling of commands for the crawler.
class DBpediaCrawler::Queue

private

  # Purge the queue (i.e., delete all messages from it).
  # For a very large number of messages, this can cause an exception 
  # "execution expired", so sleep for a while until the purge is finished.
  def purge(retries, seconds)
    puts "Purging the command queue..."
    begin
      @queue.purge
    rescue StandardError => e
      puts "Purging the query raises exception: " + e.message
      if retries > 0
        puts "Sleeping (retries: " + retries.to_s + ")..."
        sleep seconds
        retries -= 1
        retry
      else
        raise "purging failed"
      end
    end
  end

public

  # Create a new Queue
  #   configuration: hash
  def initialize(configuration)
    @config = configuration
    @queue = Bunny.new.start.create_channel.queue("semmul." + @config["agent_id"], durable: true)
    if @config["purge"] === true
      purge(@config["purge_retries"], @config["purge_sleep_seconds"])
    end
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
