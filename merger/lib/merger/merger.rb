require 'time'

class Merger::Merger
  def initialize
    publisher
    virtuoso_writer
    virtuoso_reader
  end
  
  def register_receiver
    receiver.subscribe(type: :movie_uri) { |movie_uri| merge(movie_uri) }
  end

  def merge
    # TODO: Put logic for merging here
  end

  private
  def publisher
    @publisher ||= Merger::MsgPublisher.new
  end

  def receiver
    @receiver ||= Merger::MsgConsumer.new
  end

  def virtuoso_writer
    @virtuoso_writer ||= Merger::VirtuosoWriter.new
  end

  def virtuoso_reader
    @virtuoso_reader ||= Merger::VirtuosoReader.new
  end
end
