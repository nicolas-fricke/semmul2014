require 'yaml'
require 'json'

class FreebaseMapper::Mapper


  def initialize
    @virtuoso_writer = FreebaseMapper::VirtuosoWriter.new
    @virtuoso_reader = FreebaseMapper::VirtuosoReader.new
    @receiver = FreebaseMapper::MsgConsumer.new

    puts "listening on queue #{@receiver.queue_name :movie_uri}"
    @receiver.subscribe(type: :movie_uri) { |movie_id| p movie_id }
  end
end
