require 'yaml'
require 'themoviedb'

class FreebaseUpdater::Updater

  def register_receiver
    @crawler = FreebaseUpdater::Crawler.new
    @receiver = FreebaseUpdater::MsgConsumer.new
    puts "listening on queue #{@receiver.queue_name :movie_id}"
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(movie_id)
    get_info_for_movie_with_id movie_id
    #TODO actually do something here ;)
  end

  def get_info_for_movie_with_id(mid, attempt: 0)
    query = {
      'type'=> '/film/film',
      'mid'=> mid,
      'name'=> nil,
      #'initial_release_date'=> nil,
      #'directed_by'=> [{
      #    'mid'=> nil,
      #    'name'=> nil,
      #    'film'=> [{
      #      'mid'=> nil,
      #      'name'=> nil
      #    }]
      #}],
      #'starring'=> [{
      #  'character'=> {
      #    'mid'=> nil,
      #    'name'=> nil
      #  },
      #    'actor'=> {
      #      'mid'=> nil,
      #      'name'=> nil,
      #      'film'=> [{
      #        'film'=> [{
      #          'mid'=> nil,
      #          'name'=> null
      #        }]
      #      }]
      #    }
      #}]
    }

    @crawler.execute query do |page_results|
      page_results.each do |topic|
        p topic
      end
    end
  end
end
