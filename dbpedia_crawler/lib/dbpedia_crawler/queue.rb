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
    puts "Purging the queue..."
    begin
      @queue.purge
    rescue StandardError => e
      puts "Purging the queue raises exception: " + e.message
      if retries > 0
        puts "Sleeping (retries: #{retries})..."
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
  #   type: string (type of the entities handled with this queue)
  def initialize(configuration, type)
    @config = configuration
    # create the queue
    queue_name = "#{@config["queue"]}.#{@config["agent_id"]}.#{type}"
    puts "Connecting to queue #{queue_name}..."
    @queue = Bunny.new.start.create_channel.queue(queue_name, durable: true)
    # depending on the options, purge it (i.e., remove all messages)
    if @config["purge"] === true
      purge(@config["purge_retries"], @config["purge_sleep_seconds"])
    end
  end

  # Get the next command, converted to a hash.
  #   result: hash or nil (no message)
  def pop
    begin
      message = @queue.pop[2] # [delivery info, message properties, message content]
      return message != nil ? YAML.load(message) : nil
    rescue StandardError => e
      puts "# Error while popping command from queue: " + e.message
      puts e.backtrace
      return nil
    end
  end

  # Create a command and push it to the queue. The command hash is
  # stringified using yaml (which can be parsed into a hash again without
  # using the evil "eval").
  #   hash: hash (possible keys: command, retries, arbitrary other keys)
  def push(hash)
    command = {command: nil, retries: 0}.merge hash
    begin
      @queue.publish(command.to_yaml, persistent: true)
    rescue StandardError => e
      puts "# Error while publishing command to queue:"
      p command
      puts e.message, e.backtrace
    end
  end

end
