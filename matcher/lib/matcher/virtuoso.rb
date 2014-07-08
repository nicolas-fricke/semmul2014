#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'


class Matcher::Virtuoso
    def initialize
        @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                              update_uri: 'http://localhost:8890/sparql-auth',
                                              username: secrets['databases']['virtuoso']['username'],
                                              password: secrets['databases']['virtuoso']['password'],
                                              auth_method: 'digest')
        @endpoint = 'http://localhost:8890/sparql'
        graphs()
    end

    def run_query(endpoint, query)
        result = SPARQL::Client.new(endpoint).query(query)
        yield result if block_given?
        result
    end

    def read_all()
        graph = @graphs[:raw]
        query = "select distinct * from <#{graph}> where {?s ?p ?o}"
        return run_query(@endpoint, query)
    end

    def get_all_subjects()
        graph = @graphs['raw']
        query = "select distinct ?s from <#{graph}> where {?s ?p ?o}"
        solutions = run_query(@endpoint, query)
        all_subjects = []
        solutions.each_solution do |solution|
            all_subjects << solution.to_h[:s]
        end
        return all_subjects
    end

    def get_all_for_subject(subject)
        graph = @graphs['raw']
        query = "select distinct ?p ?o from <#{graph}> where {<#{subject}> ?p ?o}"
        return run_query(@endpoint, query)
    end

    def get_triples(subject_uri)
        subject = RDF::URI.new(subject_uri)
        graph = @graphs['mapped']
        query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).graph(graph)
        triples = Matcher::Triples.new(subject)

        solutions = run_query(@endpoint, query)
        solutions.each_solution do |solution|
            triples.add_p_o(solution.bindings)
        end
        return triples
    end

    def get_entities_of_type(entity_type)
        graph = @graphs['mapped']
        uri_list = []
        if entity_type.nil?
            return []
        end
        query = "select distinct ?s from <#{graph}> where {?s ?p '#{entity_type.to_s}'.}"
        results = run_query(@endpoint, query)
        results.each_solution do |solution|
            uri_list << solution.bindings[:s]
        end
        return uri_list
    end

    def get_movie_subjects_by_imdb(imdb_id)
        s_list = []
        graph = @graphs['mapped']
        query = "select distinct ?s from <#{graph}> where {?s <http://semmul2014.hpi.de/lodofmovies.owl#imdb_id> '#{imdb_id}'. ?s rdf:type 'http://semmul2014.hpi.de/lodofmovies.owl#Movie'}"
        results = run_query(@endpoint, query)
        results.each_solution do |solution|
            s_list << solution.bindings[:s]
        end
        return s_list
    end

    def get_actor_triples(movie_triples)
        actor_triples = []
        performances = movie_triples.get_performances()
        performances.each do |p|
            p_triples = get_triples(p)
            p_actor_uris = p_triples.get_actor()
            p_actor_uris.each do |actor_uri|
                actor_triples << get_triples(actor_uri)
            end
        end
        return actor_triples
    end

    private

        def secrets
            @secrets ||= YAML.load_file '../config/secrets.yml'
        end

        def graphs
            file  ||= YAML.load_file '../config/namespaces.yml'
            @graphs = file['graphs']
        end
end
