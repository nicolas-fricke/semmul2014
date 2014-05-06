#!/usr/bin/env ruby

require 'freebase-api'
require 'json'


secrets = YAML.load_file 'secrets.yml'
ENV['GOOGLE_API_KEY'] = secrets['services']['freebase']['api_key']

def puts_json(json)
	begin
		puts JSON.pretty_generate(json)
	rescue JSON::GeneratorError => e
		puts json
	end
	json
end

def run_query(query)
	result = FreebaseAPI.session.mqlread(query)
	puts result
end

run_query({
	type: '/film/film',
	'return' => 'count'
})
# run_query [{
#   type: '/film/film',
#   mid: nil,
#   name: nil,
#   initial_release_date: nil,
#   directed_by: [{
#     mid: nil,
#     name: nil,
#     film: [{
#       mid: nil,
#       name: nil
#     }]
#   }],
#   starring: [{
#     character: {
#       mid: nil,
#       name: nil
#     },
#     actor: {
#       mid: nil,
#       name: nil,
#       film: [{
#         film: [{
#           mid: nil,
#           name: nil
#         }]
#       }]
#     }
#   }],
#   genre: [],
#   subjects: [{
#     mid: nil,
#     name: nil
#   }],
#   language: [],
#   limit: 1}]