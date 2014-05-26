require 'rubygems'
require 'cgi'
require 'httparty'
require 'json'
require 'addressable/uri'

class FreebaseCrawler
  # load API key from secrets.yml
  secrets = YAML.load_file 'secrets.yml'
  API_KEY = secrets['services']['freebase']['api_key']

  def self.retrieve_film_ids
    # retrieve mid of every topic typed film
    query = [{
                 'type' => '/film/film',
                 'mid' => nil
             }]

    # set empty cursor
    url = Addressable::URI.parse('https://www.googleapis.com/freebase/v1/mqlread')
    url.query_values = {
        'query' => query.to_json,
        'key'=> API_KEY,
        'cursor'=>nil
    }

    pages = 0
    film_ids = []
    puts "fetching elements..."
    begin
      response = HTTParty.get(url, :format => :json)
      response['result'].each { |topic|
        film_ids << topic['mid']
      }

      # set cursor from last response
      url.query_values = {
          'query' => query.to_json,
          'key'=> API_KEY,
          'cursor'=>response['cursor']
      }

      #debug output
      pages +=1
      if pages % 10 == 0
        puts "current page: #{pages}"
      end
    end until response['cursor'] == false # stop on last frame

    puts "number of fetched pages: #{pages}"
    puts "number of fetched elements: #{film_ids.size}"

    film_ids
  end
end

FreebaseCrawler.retrieve_film_ids
