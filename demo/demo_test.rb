require 'csv'
require_relative '../general/virtuoso_reader'

def test_demo_data(path)
  p path
  movie_links = []
  missing_count = [0,0,0]
  counter = 1
  reader = VirtuosoReader.new graph:'raw'
  CSV.foreach(path, { :col_sep => "\t" }) do |dbpedia_link, fb_link, tmdb_link|

    begin
      dbpedia_exists = reader.exists_subject subject: dbpedia_link

      fb_mid = fb_link.split("freebase.com/m/")[1]
      fb_uri = "http://rdf.freebase.com/ns/m/#{fb_mid}"
      fb_exists = reader.exists_subject subject: fb_uri

      tmdb_id = tmdb_link.split("themoviedb.org/")[1].split("/")[1][/\d+/]
      tmdb_uri = "http://semmul2014.hpi.de/tmdb/movie/#{tmdb_id}"
      tmdb_exists = reader.exists_subject subject: tmdb_uri

      missing_count[0] += 1 unless dbpedia_exists
      missing_count[1] += 1 unless fb_exists
      missing_count[2] += 1 unless tmdb_exists

    rescue Exception => e
      puts e
    end
    $stdout.write "\r#{counter}/1000"
    $stdout.flush
    counter += 1
  end
  puts ""
  puts missing_count

end

DEFAULT_PATH = '../demo/1000_Movies.tsv'


test_demo_data(File.absolute_path DEFAULT_PATH)
