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
    enqueue type, id
  end

  def enqueue_uri(type, uri)
    enqueue type, uri
  end

  def enqueue(type, payload)
    p "writing to #{queue_name(type)}: #{payload}"
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue queue_name(type), durable: true
    queue.publish payload.to_s, persistent: true
    @connection.close
  end
end
