require 'bunny'

class MsgPublisher
  def initialize
    @connection = Bunny.new
  end

  def set_queue(queue)
    @queue = queues[queue.to_s]
  end

  def enqueue(type, id)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue "#{@queue}#{type.to_s}", durable: true
    queue.publish id.to_s, persistent: true
    @connection.close
  end

  def queue_name(type)
    "#{@queue}#{type.to_s}"
  end

  private
  def queues
    @queues ||= YAML.load_file('../config/namespaces.yml')['queues']
  end

end
