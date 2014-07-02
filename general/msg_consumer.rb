require 'bunny'

class MsgConsumer
  def initialize
    @connection = Bunny.new
  end

  def set_queue(queue)
    @queue = queues[queue.to_s]
  end

  def subscribe(type: type, &block)
    @connection.start
    channel = @connection.create_channel
    channel.prefetch 1 # Only take one message at a time, leave others on queue
    queue   = channel.queue "#{@queue}#{type.to_s}", durable: true
    queue.subscribe(manual_ack: true, block: true) do |delivery_info, properties, body|
      block.call body
      channel.ack delivery_info.delivery_tag
    end
  rescue Interrupt => _
    @connection.close
    puts 'Received Interrupt, closed connection.'
  end

  private
  def queues
    file ||= YAML.load_file '../config/namespaces.yml'
    @queues = file['queues']
  end
end
