require 'bunny'

class MsgReceiver
  def initialize
    @connection = Bunny.new
  end

  def subscribe(type, &block)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue "source.tmdb.#{type.to_s}"
    queue.subscribe(block: true) do |delivery_info, properties, body|
      block.call body
      puts 'oi'
      # delivery_info.consumer.cancel
    end
    # @connection.close
  end
end
