require 'csv'
require_relative '../lib/freebase_updater'

def parse_movie_file(path)
  p path
  movie_links = []
  CSV.foreach(path) do |name, _, fb_link, _|
    movie_links << fb_link
  end
  movie_links.map do |l|
    match = l.match /.*(?<mid>\/m\/.*)/
    match[:mid]
  end.reject {|l| l.nil? }
end

DEFAULT_PATH = '../demo/movie_links.csv'

if ARGV.first
  FreebaseUpdater::Updater.new.start_demo parse_movie_file(ARGV.first)
elsif File.exists? DEFAULT_PATH
  FreebaseUpdater::Updater.new.start_demo parse_movie_file(File.absolute_path DEFAULT_PATH)
else
  p "please specify a cvs file"
end