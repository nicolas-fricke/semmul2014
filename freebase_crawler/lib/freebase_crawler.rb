# taken from https://developers.google.com/freebase/v1/mql-overview
require 'rubygems'
require 'cgi'
require 'httparty'
require 'json'
require 'addressable/uri'
require 'yaml'

module FreebaseCrawler
  class FreebaseCrawler::Crawler
    def read_mql(query, page: 1, verbose: false, cursor: nil, &block)
      url = Addressable::URI.parse('https://www.googleapis.com/freebase/v1/mqlread')
      url.query_values = {
          'query' => query.to_json,
          'key'=> api_key,
          'cursor'=>cursor
      }
      begin
        response = HTTParty.get(url, :format => :json)

        #p response
        # process results
        if response['result'].nil?
          # for some reason sometimes the result is nil...
          p ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> The result is nil, response was #{response}"
        else
          yield response['result'] if block_given?
        end

        # set cursor from last response
        url.query_values = {
            'query' => query.to_json,
            'key'=> api_key,
            'cursor'=>response['cursor']
        }

        #debug output
        puts "fetching elements, current page: #{page}" if verbose

        if response['cursor']  # is false if no more pages available
          execute query, page: page + 1, cursor: response['cursor'],  &block
        end

      rescue SocketError => e
        p "SocketError occured: #{e} --> repeating query"
        sleep 0.5
        execute query, page: page, cursor: cursor
      end
    end

    def read_topic(topic_id, filter: nil, &block)
      url = Addressable::URI.parse('https://www.googleapis.com/freebase/v1/topic' + topic_id)
      url.query_values = {
          'key'=> api_key
      }
      url.query_values[:filter] = filter if filter
      begin
        response = HTTParty.get(url, :format => :json)
        yield response['property'] if block_given?

      rescue SocketError => e
        p "SocketError occured: #{e} --> repeating query"
        sleep 0.5
        read_topic topic_id, block
      end
    end

    private
    def api_key
      @secrets ||= YAML.load_file '../../freebase_crawler/config/secrets.yml'
      @secrets['services']['freebase']['api_key']
    end
  end
end
