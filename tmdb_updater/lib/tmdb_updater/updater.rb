require 'yaml'
require 'themoviedb'

class TMDbUpdater::Updater
  def initialize
    initialize_tmdb_api
  end

  def register_receiver
    @receiver = TMDbUpdater::MsgConsumer.new
    @receiver.subscribe(type: :movie_id) { |movie_id| update(movie_id) }
  end

  def update(movie_id)
    movie_details = get_info_for_movie_with_id movie_id
    puts movie_details
  end

  def get_info_for_movie_with_id(movie_id, attempt: 0)
    Tmdb::Movie.detail movie_id
  rescue SocketError => e
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      sleep 0.5
      get_info_for_movie_with_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, cancel retry for movie with id #{movie_id}"
      raise e
    end
  end

  private
  def secrets
    @secrets ||= YAML.load_file 'config/secrets.yml'
  end

  def initialize_tmdb_api
    Tmdb::Api.key secrets['services']['tmdb']['api_key']
  end
end
