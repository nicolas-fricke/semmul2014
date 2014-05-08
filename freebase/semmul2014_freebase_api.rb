#!/usr/bin/env ruby

require 'freebase-api'
require 'json'

module Semmul2014FreebaseAPI

  secrets = YAML.load_file 'secrets.yml'
  ENV['GOOGLE_API_KEY'] = secrets['services']['freebase']['api_key']

  def self.puts_json(json)
    begin
      puts JSON.pretty_generate(json)
    rescue JSON::GeneratorError => e
      puts json
    end
    json
  end

  def self.run_query(query)
    result = FreebaseAPI.session.mqlread(query)
    yield result if block_given?
    result
  end

end
