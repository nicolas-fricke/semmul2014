class Matcher::Triples
    attr_accessor :subject

    def initialize(subject)
      @subject = subject
      @data = []
      config
    end

    def to_s
      name = get_name()
      unless name.nil?
        "<Matcher::Triples (#{name.to_s}) #{@subject.to_s}>"
      else
        "<Matcher::Triples #{@subject.to_s}>"
      end
    end

    def add_p_o(p_o_hash)
      @data << p_o_hash
    end

    def get_objects(p_uri)
      result = []
      @data.each do |p_o_hash|
        if p_o_hash[:p] == p_uri
          result << p_o_hash[:o]
        end
      end
      result
    end

    def get_type
      get_objects(RDF.type).each do |type|
        result = case type
          when @types['movie_type']
            RDF::URI.new(@types['movie_type'])
          when @types['person_type']
            RDF::URI.new(@types['person_type'])
          when @types['organization_type']
            RDF::URI.new(@types['organization_type'])
          when @types['director_type']
            RDF::URI.new(@types['director_type'])
          when @types['performance_type']
            RDF::URI.new(@types['performance_type'])
          end
        return result if result
      end
      nil # no type found
    end

    def get_name()
      get_objects("http://schema.org/name").first
    end

    def get_names()
      get_objects("http://schema.org/name")
    end

    def get_given_name()
      get_objects("http://schema.org/givenName")
    end

    def get_family_name()
      get_objects("http://schema.org/familyName")
    end

    def get_alternative_names()
      get_objects("http://schema.org/alternateName")
    end

    def get_birthdate()
      birthdate_literals = get_objects("http://schema.org/birthDate")
      if birthdate_literals.size > 0
        birthdate_literal =  birthdate_literals.first
        _parse_date(birthdate_literal)
      else
        nil
      end
    end

    def _parse_date(literal_string)
      literal_string = literal_string.to_s.split("^^").first
      # only one number (e.g. "1934") --> use July 1. as date
      begin
        if _only_one_number(literal_string)
          Date.parse(literal_string+"-07-01")
        else
          Date.parse(literal_string)
        end
      rescue ArgumentError
        nil
      end
    end

    def _only_one_number(string)
      number = !/\d+/.match(string).nil?
      no_letters = /[a-zA-Z]+/.match(string).nil?
      no_non_digits = /\D+/.match(string).nil?

      number and no_letters and no_non_digits
    end

    def get_imdb_id()
      objects = get_objects("http://semmul2014.hpi.de/lodofmovies.owl#imdb_id")
      objects.first unless objects.empty?
    end

    def get_fb_mid()
      fb_mid_prop = "http://semmul2014.hpi.de/lodofmovies.owl#freebase_mid"
      objects = get_objects(fb_mid_prop)
      objects.first unless objects.empty?
    end

    def get_release_date()
      release_date_prop = "http://schema.org/datePublished"
      releasedate_literals = get_objects(release_date_prop)
      _parse_date releasedate_literals.first unless releasedate_literals.empty?
    end

    def get_performances()
      performance_prop = "http://semmul2014.hpi.de/lodofmovies.owl#performance"
      get_objects performance_prop
    end

    def get_actor()
      actor_prop = "http://semmul2014.hpi.de/lodofmovies.owl#actor"
      get_objects actor_prop
    end

    def get_director()
      director_uris = get_objects("http://schema.org/director")
      director_uris.first unless director_uris.empty?
    end

    def get_character()
      char_uris = get_objects("http://semmul2014.hpi.de/lodofmovies.owl#character")
      char_uris.first unless char_uris.empty?
    end

    private

    def config
      @namespaces ||= YAML.load_file '../config/namespaces.yml'
      @types = @namespaces['types']
    end

end