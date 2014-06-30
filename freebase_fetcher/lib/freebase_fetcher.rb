module FreebaseFetcher
  require_relative '../../freebase_crawler/lib/freebase_crawler'
  require_relative '../../general/msg_publisher'

  class FreebaseFetcher::Fetcher
    def initialize
      @crawler = FreebaseCrawler::Crawler.new
      @publisher = MsgPublisher.new
      @publisher.set_queue 'source_freebase'
    end

    def retrieve_film_ids(verbose: false)
      # retrieve mid of every topic typed film
      query = [{
                   'type' => '/film/film',
                   'mid' => nil
               }]

      @crawler.read_mql query do |page_results|
        page_results.each do |topic|
          # write film_id to queue
          p "publishing #{topic['mid']} to #{@publisher.queue_name :movie_id}" if verbose
          @publisher.enqueue :movie_id, topic['mid']
        end
      end
    end
  end
end
