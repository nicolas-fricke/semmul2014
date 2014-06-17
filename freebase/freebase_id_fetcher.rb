require 'bunny'
require_relative 'freebase_crawler'


def retrieve_film_ids
  crawler = FreebaseCrawler.new

  #open film_id_queue on localhost
  connection = Bunny.new(:automatically_recover => false).start
  film_id_queue = connection.create_channel.queue("semmul.film_ids.freebase", :durable => true)

  # retrieve mid of every topic typed film
  query = [{
               'type' => '/film/film',
               #'type' => '/fashion/fashion_designer',
               'mid' => nil
           }]

  @film_ids = []
  crawler.execute query do |page_results|
    page_results.each do |topic|
      @film_ids << topic['mid']
      # write film_id to queue
      p "publishing #{topic['mid']} to #{film_id_queue.name}"
      film_id_queue.publish(topic['mid'], :persistent => true)
    end
  end

  @film_ids
end


retrieve_film_ids
