require 'yaml'
require 'json'

class FreebaseUpdater::Updater
  def initialize
    @crawler = FreebaseCrawler::Crawler.new
    @virtuoso = FreebaseUpdater::VirtuosoWriter.new
    @receiver = FreebaseUpdater::MsgConsumer.new

    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(movie_id)
    # long queries are not necessarily answered, thus we split it
    p "Looking up MID #{movie_id} ..."
    retrieve_topic movie_id do |topic|
      write_triples_for movie_id, topic
    end

  end

  def write_triples_for(movie_id, topic)

    ns = 'http://rdf.freebase.com/ns'

    key = '/type/object/name'
    retrieve_element topic, key do |name|
      @virtuoso.new_triple ns+movie_id, ns+key, name
    end

  end

  def retrieve_topic(topic_id)
    @crawler.read_topic topic_id do |topic|
      yield topic if block_given?
    end
  end

  def retrieve_element(topic_response, locator)
    element = topic_response[locator]['values'].first['value']
    yield element if block_given?
    element
  end
end
