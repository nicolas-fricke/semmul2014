require 'csv'
require_relative '../lib/freebase_mapper'
DEFAULT_PATH = '../demo/movie_links.csv'

def parse_movie_file(path)
  p path
  movie_links = []
  CSV.foreach(path) do |name, _, fb_link, _, _|
    movie_links << fb_link
  end
  movie_links
end


if ARGV.first
  FreebaseMapper::Mapper.new.start_demo parse_movie_file(ARGV.first)
elsif File.exists? DEFAULT_PATH
  FreebaseMapper::Mapper.new.start_demo parse_movie_file(File.absolute_path DEFAULT_PATH)
else
  p "please specify a cvs file"
end