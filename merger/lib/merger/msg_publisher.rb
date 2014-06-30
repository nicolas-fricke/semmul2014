require 'bunny'

# Klasse ist zum Testen gedacht
# damit Queue ihre Elemente behält und sie nicht immer wieder neu gefüllt werden muss für den nächsten Test

class Merger::MsgPublisher
  def initialize
    @connection = Bunny.new
  end

  def enqueue_uri(type, uri)
    @connection.start
    channel = @connection.create_channel
    queue   = channel.queue "lom.merged.#{type.to_s}", durable: true
    queue.publish uri.to_s, persistent: true
    @connection.close
  end
end
