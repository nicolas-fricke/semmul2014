require 'bunny'

class FreebaseMapper::MsgConsumer
  def initialize
    @connection = Bunny.new
  end

  def name
    'freebase'
  end

  def queue_name(type)
    "lom.raw_db.#{name}.#{type.to_s}"
  end

  def subscribe(type: :movie_uri, &block)
    @connection.start
    channel = @connection.create_channel
    channel.prefetch 1 # Only take one message at a time, leave others on queue
    queue   = channel.queue queue_name(type), durable: true
    queue.subscribe(manual_ack: true, block: true) do |delivery_info, properties, body|
      block.call body
      channel.ack delivery_info.delivery_tag
    end
  rescue Interrupt => _
    @connection.close
    puts 'Received Interrupt, closed connection.'
  end
end
