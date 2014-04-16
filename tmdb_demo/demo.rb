require 'themoviedb'
require 'sourcify'
require 'yaml'
require 'awesome_print'

secrets = YAML.load_file 'secrets.yml'
Tmdb::Api.key secrets['services']['tmdb']['api_key']

def print_and_execute_command(&block)
  puts "Executing: #{block.to_source}   [press enter...]"
  gets
  result = block.call
  puts "Result:"
  awesome_print result
  puts '[press enter...]'
  gets
  puts "\n\n"
end

print_and_execute_command { Tmdb::Movie.find 'Men in Black' }

first_men_in_black_movie = Tmdb::Movie.find('Men in Black').first
print_and_execute_command { Tmdb::Movie.alternative_titles first_men_in_black_movie.id }
print_and_execute_command { Tmdb::Movie.casts first_men_in_black_movie.id }

print_and_execute_command { Tmdb::Movie.upcoming }
