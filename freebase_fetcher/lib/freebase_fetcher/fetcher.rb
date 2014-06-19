class FreebaseFetcher::Fetcher
  def initialize
    @crawler = FreebaseFetcher::Crawler.new
    @publisher = FreebaseFetcher::MsgPublisher.new
  end

  def retrieve_film_ids
    # retrieve mid of every topic typed film
    query = [{
                 'type' => '/film/film',
                 #'type' => '/fashion/fashion_designer',
                 'mid' => nil
             }]

    @film_ids = []
    @crawler.execute query do |page_results|
      page_results.each do |topic|
        @film_ids << topic['mid']
        # write film_id to queue
        p "publishing #{topic['mid']} to #{@publisher.queue_name :movie_id}"
        @publisher.enqueue_id :movie_id, topic['mid']
      end
    end

    @film_ids
  end
end
