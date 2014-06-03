require 'bunny'

class MsgPublisher
  def initialize
    @connection = Bunny.new
  end

  def enqueue_id(type, id)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue "source.tmdb.#{type.to_s}"
    channel.default_exchange.publish id.to_s, routing_key: queue.name
    @connection.close
  end
end
