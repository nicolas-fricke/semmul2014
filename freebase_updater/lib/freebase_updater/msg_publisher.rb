require 'bunny'

class FreebaseUpdater::MsgPublisher
  def initialize
    @connection = Bunny.new
  end

  def name
    'freebase'
  end

  def queue_name(type)
    "lom.raw_db.#{name}.#{type.to_s}"
  end

  def enqueue_id(type, id)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue queue_name(type), durable: true
    queue.publish id.to_s, persistent: true
    @connection.close
  end
end
