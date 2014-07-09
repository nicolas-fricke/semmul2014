require 'csv'

class TMDbFetcher::Demo
  def initialize(demo_file_path)
    @publisher = MsgPublisher.new
    @publisher.set_queue 'source_tmdb'

    movie_ids = parse_movie_file(demo_file_path)
    movie_ids.each do |movie_id|
      enqueue_movie movie_id
    end
  end

  def parse_movie_file(path)
    movie_links = []
    CSV.foreach(path) do |name, _, _, tmdb_link|
      movie_links << tmdb_link
    end
    movie_links.map do |l|
      l.match /.*\/(\d*)[^\d]*/
      $1
    end.reject {|l| l.nil? }
  end

  def enqueue_movie(movie_id)
    @publisher.enqueue :movie_id, movie_id
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end
end
