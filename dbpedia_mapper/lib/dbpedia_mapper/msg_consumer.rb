require 'bunny'

class DBpediaMapper::MsgConsumer
  def initialize
    @connection = Bunny.new
  end

  def subscribe(type=type, &block)
    @connection.start
    channel = @connection.create_channel
    channel.prefetch 1 # Only take one message at a time, leave others on queue
    queue = channel.queue "lom.raw_db.dbpedia.#{type.to_s}", durable: true
    queue.subscribe(manual_ack: true, block: true) do |delivery_info, properties, body|
      block.call body
      channel.ack delivery_info.delivery_tag
    end
  rescue Interrupt => _
    @connection.close
    puts 'Received Interrupt, closed connection.'
  end
end
