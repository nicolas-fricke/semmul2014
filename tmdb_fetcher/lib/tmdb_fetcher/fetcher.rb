require 'yaml'
require 'themoviedb'

class TMDbFetcher::Fetcher
  def initialize
    @publisher = TMDbFetcher::MsgPublisher.new
    initialize_tmdb_api
  end

  def fetch_movies_recursively(page: 1, verbose: false)
    tmdb_search = Tmdb::Search.new('/discover/movie')
    tmdb_search.filter(page: page)
    tmdb_result = tmdb_search.fetch_response
    if tmdb_result['results'].nil?
      return
    else
      tmdb_result['results'].each do |movie|
        @publisher.enqueue_id :movie_id, movie['id']
      end
    end

    if page < tmdb_result['total_pages']
      next_page = page + 1
    else
      next_page = 1
    end

    puts "  Fetched page #{page}, continuing with page #{next_page}" if verbose

    fetch_movies_recursively(page: next_page, verbose: verbose)
  rescue SocketError
    puts '  !!! A socket error occurred, retrying'
    sleep 0.5
    fetch_movies_recursively(page: page, verbose: verbose)
  end

  def fetch_movie_ids(verbose: false)
    puts 'Start fetching movie ids...' if verbose

    fetch_movies_recursively(verbose: verbose)
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def initialize_tmdb_api
    Tmdb::Api.key secrets['services']['tmdb']['api_key']
  end
end
