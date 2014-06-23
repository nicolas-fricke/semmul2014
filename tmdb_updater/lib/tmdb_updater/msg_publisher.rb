require 'bunny'

class TMDbUpdater::MsgPublisher
  def initialize
    @connection = Bunny.new
  end

  def enqueue_id(type, id)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue "lom.raw_db.tmdb.#{type.to_s}", durable: true
    queue.publish id.to_s, persistent: true
    @connection.close
  end
end
