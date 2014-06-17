# taken from https://developers.google.com/freebase/v1/mql-overview
require 'rubygems'
require 'cgi'
require 'httparty'
require 'json'
require 'addressable/uri'


class FreebaseCrawler
  # load API key from secrets.yml
  secrets = YAML.load_file 'secrets.yml'
  API_KEY = secrets['services']['freebase']['api_key']

  def initialize

  end

  def execute(query)
    # set empty cursor
    url = Addressable::URI.parse('https://www.googleapis.com/freebase/v1/mqlread')
    url.query_values = {
        'query' => query.to_json,
        'key'=> API_KEY,
        'cursor'=>nil
    }

    pages = 0
    error_counter = 0
    puts "fetching elements..."
    begin
      response = HTTParty.get(url, :format => :json)

      # process results
      yield response['result'] if block_given?

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
    rescue SocketError => e
      error_counter += 1
      if error_counter <= 3
        p "SocketError occured: #{e} --> repeating query"
        redo
      else
        error_counter = 0
        p "Too many errors --> stop"
        return
      end
    end until response['cursor'] == false # stop on last frame
  end
end

