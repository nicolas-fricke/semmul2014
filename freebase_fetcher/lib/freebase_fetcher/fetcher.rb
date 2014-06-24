class FreebaseFetcher::Fetcher
  def initialize
    #@crawler = FreebaseFetcher::Crawler.new
    @crawler = FreebaseCrawler::Crawler.new
    @publisher = FreebaseFetcher::MsgPublisher.new
  end

  def retrieve_film_ids(verbose: false)
    # retrieve mid of every topic typed film
    query = [{
                 'type' => '/film/film',
                 #'type' => '/fashion/fashion_designer', # testing
                 'mid' => nil
             }]

    @crawler.execute query do |page_results|
      page_results.each do |topic|
        # write film_id to queue
        p "publishing #{topic['mid']} to #{@publisher.queue_name :movie_id}" if verbose
        @publisher.enqueue_id :movie_id, topic['mid']
      end
    end
  end
end
