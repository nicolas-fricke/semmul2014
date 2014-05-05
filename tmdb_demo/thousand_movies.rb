require 'yaml'
require 'themoviedb'

class MovieFetcher
  def secrets
    @secrets ||= YAML.load_file 'secrets.yml'
  end

  def initialize_tmdb_api
    Tmdb::Api.key secrets['services']['tmdb']['api_key']
  end

  def fetch_movies_recursively(requested_movie_count=1000, fetched_ids: [], page: 1, verbose: false)
    puts "  #{fetched_ids.count} movies fetched, continuing" if verbose
    fetched_ids.concat Tmdb::Movie.discover(page: page).map { |movie| movie['id'] }

    if fetched_ids.count < requested_movie_count
      fetched_ids = fetch_movies_recursively(requested_movie_count, fetched_ids: fetched_ids, page: page + 1, verbose: verbose)
    end

    fetched_ids
  rescue SocketError
    puts "  !!! A socket error occurred, retrying"
    fetch_movies_recursively(requested_movie_count, fetched_ids: fetched_ids, page: page, verbose: verbose)
  end

  def fetch_n_movie_ids(requested_movie_count=1000, verbose: false)
    puts "Start fetching #{requested_movie_count} movie ids..." if verbose

    fetched_ids = fetch_movies_recursively(requested_movie_count, verbose: verbose)

    puts "Done fetching movies, got #{fetched_ids.count} ids, returning the first #{requested_movie_count}." if verbose
    fetched_ids[0...requested_movie_count]
  end

  def get_info_for_movie_with_id(movie_id, attempt: 0)
    Tmdb::Movie.detail movie_id
  rescue SocketError
    if attempt < 10
      puts "  !!! A socket error occurred, retrying (#{attempt} retries already, id: #{movie_id})"
      get_info_for_movie_with_id movie_id, attempt: (attempt + 1)
    else
      puts "  !!! A socket error occurred 10 times, skipping movie with id #{movie_id}"
    end
  end

  def get_info_for_movie_with_ids(movie_ids, verbose: false)
    puts "Start fetching details for #{movie_ids.count} movies..." if verbose
    details = movie_ids.each_with_index.map do |movie_id, index|
      puts "  Fetching details for movie #{movie_id} (#{index}/#{movie_ids.count})" if verbose
      get_info_for_movie_with_id movie_id
    end
    puts "Done fetching details for #{movie_ids.count} movies." if verbose
    details
  end

  def calculate_tmdb_info_quota(movie_details, verbose: false)
    movies_with_imdb_id = movie_details.select do |details|
      details.imdb_id != ''
    end
    quota = movies_with_imdb_id.count.fdiv movie_details.count
    puts "#{movies_with_imdb_id.count} of the #{movie_details.count} have a reference to IMDB. That is #{(quota * 100).round(2)}" if verbose
  end
end

movie_fetcher = MovieFetcher.new
movie_fetcher.initialize_tmdb_api
movie_ids     = movie_fetcher.fetch_n_movie_ids 1000, verbose: true
movie_details = movie_fetcher.get_info_for_movie_with_ids movie_ids, verbose: true
movie_fetcher.calculate_tmdb_info_quota movie_details, verbose: true


