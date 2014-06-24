require 'yaml'
require 'json'

class FreebaseUpdater::Updater
  def initialize
    @crawler = FreebaseCrawler::Crawler.new
    @receiver = FreebaseUpdater::MsgConsumer.new
    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(movie_id)
    # long queries are not necessarily answered, thus we split it
    p "Looking up MID #{movie_id} ..."
    retrieve_topic movie_id
    #TODO actually do something here ;)
  end

  def retrieve_topic(topic_id)
    @crawler.read_topic topic_id do |topic|
      puts JSON.pretty_generate(topic)
      #p topic
    end
  end
end
