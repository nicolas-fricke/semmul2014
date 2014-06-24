require 'bunny'

class TMDbUpdater::MsgPublisher
  def initialize
    @connection = Bunny.new
  end

  def enqueue_uri(type, uri)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue "lom.raw_db.tmdb.#{type.to_s}", durable: true
    queue.publish uri.to_s, persistent: true
    @connection.close
  end
end
