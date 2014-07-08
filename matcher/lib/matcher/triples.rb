class Matcher::Triples

    # todo: check if source is tmdb, dbpedia or freebase

    attr_accessor :subject

    def initialize(subject)
        @subject = subject
        @data = []
    end


    def to_s
        name = get_name()
        if !name.nil?
            return "<Matcher::Triples (" + name.to_s + ") " + @subject.to_s + ">"
        else
            return "<Matcher::Triples " + @subject.to_s + ">"
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
        return result
    end

    def get_type()
        movie = "http://semmul2014.hpi.de/lodofmovies.owl#Movie"

        types = get_objects(RDF.type)
        types.each do |type|
            if type.to_s == movie
                return RDF::URI.new(movie)
            end
        end
        return nil
    end

    def get_name()
        name_p = "http://schema.org/name"
        return get_objects(name_p)[0]
    end

    def get_alternative_names()
        alternate_name_prop = "http://schema.org/alternateName"
        return get_objects(alternate_name_prop)
    end

    def get_birthdate()
        birthdate_prop = "http://schema.org/birthDate"
        birthdate_literals = get_objects(birthdate_prop)
        if birthdate_literals.size > 0
            birthdate_literal =  birthdate_literals[0]
            return _parse_date(birthdate_literal)
        else
            return nil
        end
    end

    def _parse_date(literal_string)
        literal_string = literal_string.to_s.split("^^").first
        # only one number (e.g. "1934") --> use July 1. as date
        begin
            if _only_one_number(literal_string)
                return Date.parse(literal_string+"-07-01")
            else
                return Date.parse(literal_string)
            end
        rescue ArgumentError
            return nil
        end
    end

    def _only_one_number(string)
        number = !/\d+/.match(string).nil?
        no_letters = /[a-zA-Z]+/.match(string).nil?
        no_non_digits = /\D+/.match(string).nil?

        return (number and no_letters and no_non_digits)
    end


    def get_imdb_id()
        imdb_prop = "http://semmul2014.hpi.de/lodofmovies.owl#imdb_id"
        objects = get_objects(imdb_prop)
        if objects.size > 0
            return objects[0]
        else
            return nil
        end
    end

    def get_release_date()
        # todo: not in tmdb
        release_date_prop = "http://schema.org/datePublished"
        releasedate_literals = get_objects(release_date_prop)
        if releasedate_literals.size > 0
            return _parse_date(releasedate_literals[0])
        end
    end

    def get_performances()
        performance_prop = "http://semmul2014.hpi.de/lodofmovies.owl#performance"
        performance_uris = get_objects(performance_prop)
        return performance_uris
    end

    def get_actor()
        actor_prop = "http://semmul2014.hpi.de/lodofmovies.owl#actor"
        actor_uris = get_objects(actor_prop)
        return actor_uris
    end


end