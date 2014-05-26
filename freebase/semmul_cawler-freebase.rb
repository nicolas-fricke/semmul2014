# taken from https://developers.google.com/freebase/v1/mql-overview
require 'rubygems'
require 'cgi'
require 'httparty'
require 'json'
require 'addressable/uri'

require 'bunny'

class FreebaseCrawler
  # load API key from secrets.yml
  secrets = YAML.load_file 'secrets.yml'
  API_KEY = secrets['services']['freebase']['api_key']

  def initialize
    #open film_id_queue on localhost
    connection = Bunny.new(:automatically_recover => false).start
    @film_id_queue = connection.create_channel.queue("semmul.film_ids.freebase", :durable => true)
  end

  def retrieve_film_ids
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
        # write film_id to queue
        @film_id_queue.publish(topic['mid'], :persistent => true)
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

    film_ids
  end
end

crawler = FreebaseCrawler.new
crawler.retrieve_film_ids
