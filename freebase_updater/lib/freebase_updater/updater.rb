require 'yaml'
require 'json'

class FreebaseUpdater::Updater
  def initialize
    @crawler = FreebaseUpdater::Crawler.new
    @receiver = FreebaseUpdater::MsgConsumer.new
    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(movie_id)
    # long queries are not necessarily answered, thus we split it
    p "Looking up MID #{movie_id} ..."
    film_info_primitives movie_id
    film_info_actor movie_id

    #TODO actually do something here ;)
  end



  def film_info_primitives(mid)
    # look up data on film itself (mostly primitives)
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

  def film_info_actor(mid)
    query = {
        'type'=> '/film/film',
        'mid'=> mid,
        'starring'=> [{
          'actor'=> {
            'mid'=> nil,
            'name'=> nil,
          }
        }]
    }


    @crawler.execute query do |topic|
      #puts JSON.pretty_generate(topic)
      p topic
    end
  end
end


#def film_info_actor(mid)
#  query = {
#      'type'=> '/film/film',
#      'mid'=> mid,
#      'directed_by'=> [{
#                           'mid'=> nil,
#                           #'name'=> nil,
#                       }],
#      'starring'=> [{
#                        'character'=> {
#                            'mid'=> nil,
#                            #'name'=> nil
#                        },
#                        'actor'=> {
#                            'mid'=> nil,
#                            #'name'=> nil,
#                        }
#                    }]
#  }
