#!/usr/bin/env ruby
require 'sparql/client'
require 'rdf'
require 'logger'

class TMDbMapper::DBpediaReader
  def initialize
    load_schemas
    @log = Logger.new('log', 'daily')
    @sparql = SPARQL::Client.new("http://dbpedia.org/sparql/")
  end

  def get_place_uri(place_string)
    type = RDF::URI.new("#{@schemas['rdf']}type")
    place = RDF::URI.new("#{@schemas['dbpedia']}Place")
    label = RDF::URI.new("#{@schemas['rdfs']}label")
    contains = RDF::URI.new("bif:contains")

    # encode (because sparql cannot handle these characters)
    fallback = {
      'Š'=>'S', 'š'=>'s', 'Ð'=>'Dj','Ž'=>'Z', 'ž'=>'z', 'À'=>'A', 'Á'=>'A', 'Â'=>'A', 'Ã'=>'A', 'Ä'=>'A',
      'Å'=>'A', 'Æ'=>'A', 'Ç'=>'C', 'È'=>'E', 'É'=>'E', 'Ê'=>'E', 'Ë'=>'E', 'Ì'=>'I', 'Í'=>'I', 'Î'=>'I',
      'Ï'=>'I', 'Ñ'=>'N', 'Ò'=>'O', 'Ó'=>'O', 'Ô'=>'O', 'Õ'=>'O', 'Ö'=>'O', 'Ø'=>'O', 'Ù'=>'U', 'Ú'=>'U',
      'Û'=>'U', 'Ü'=>'U', 'Ý'=>'Y', 'Þ'=>'B', 'ß'=>'Ss','à'=>'a', 'á'=>'a', 'â'=>'a', 'ã'=>'a', 'ä'=>'a',
      'å'=>'a', 'æ'=>'a', 'ç'=>'c', 'è'=>'e', 'é'=>'e', 'ê'=>'e', 'ë'=>'e', 'ì'=>'i', 'í'=>'i', 'î'=>'i',
      'ï'=>'i', 'ð'=>'o', 'ñ'=>'n', 'ò'=>'o', 'ō'=>'o', 'ó'=>'o', 'ô'=>'o', 'õ'=>'o', 'ö'=>'o', 'ø'=>'o', 'ù'=>'u',
      'ú'=>'u', 'û'=>'u', 'ý'=>'y', 'ý'=>'y', 'þ'=>'b', 'ÿ'=>'y', 'ƒ'=>'f'
    }
    begin
      place_string = place_string.to_s.encode('us-ascii', :fallback => fallback)
    rescue Exception => e
      @log.error "Cannot convert place '#{place_string}' to US-ASCII. Probably the string contains characters not included in fallback."
    end
    place_string.to_s.delete! '.'
    tokens = place_string.to_s.scan(/[A-Za-z0-9]+/)

    # first try with all tokens in string
    string = tokens.join(' AND ')
    puts place_string

    begin
      query = @sparql.select.where([:s, label, :label]).where([:label, contains, string]).where([:s, type, place]).limit(1)

      if query.each_solution.count > 0
        query.each_solution do |solution|
          puts solution.bindings[:s]
          return solution.bindings[:s]
        end
      elsif Array(tokens).count > 2
        puts 'again (0, 1) ...'
        string = RDF::Literal.new("#{Array(tokens).first}, #{Array(tokens)[1]}", :language => :en)
        query = @sparql.select.where([:s, label, string]).where([:s, type, place]).limit(1)
        if query.each_solution.count > 0
          query.each_solution do |solution|
            puts solution.bindings[:s]
            return solution.bindings[:s]
          end
        else
          puts 'again (0 AND last) ...'
          get_place_uri "#{Array(tokens).first} #{Array(tokens).last}"
        end
      else
        puts 'nil'
        @log.info "The Place '#{place_string}' could not be mapped to a DBpedia entity."
        return nil
      end
    rescue Exception => e
      puts e
      @log.error e
    end
  end

  def load_schemas
    file  ||= YAML.load_file '../config/namespaces.yml'
    @schemas = file['schemas']
  end
end