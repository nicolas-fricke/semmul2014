require 'yaml'
require 'json'

class FreebaseUpdater::Updater

  def register_receiver
    @crawler = FreebaseUpdater::Crawler.new
    @receiver = FreebaseUpdater::MsgConsumer.new
    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(movie_id)
    # long queries are not necessarily answered, thus we split it
    film_info_1 movie_id

    #TODO actually do something here ;)
  end

  def film_info_1 (mid)
    # look up data on film itself (mostly primitives)
    p "Looking up MID #{mid}..."
    query = {
            'type'=> '/film/film',
            'mid'=> mid,
            'name'=> nil,
            'initial_release_date'=> nil,
            'genre'=> [],
            #'runtime'=> [{
            #   'runtime'=> nil,
            #}],
            'language'=> [],
            #'tagline'=>[],
            'estimated_budget'=>nil,
            #'rating'=>[{
            #  'minimum_unaccompanied_age'=>nil # this is specific to the country...
            #           }],
            'trailers'=>[],
            'netflix_id'=>[],
            'nytimes_id'=>[],
            'metacritic_id'=>[],
            'apple_movietrailer_id'=>[],
            'rottentomatoes_id'=>[]
        }

    @crawler.execute query do |topic|
      puts JSON.pretty_generate(topic)
      #p topic
    end
  end
end
