#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'

class Matcher::Virtuoso
  def initialize(evaluation=false)
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                          update_uri: 'http://localhost:8890/sparql-auth',
                                          username: secrets['databases']['virtuoso']['username'],
                                          password: secrets['databases']['virtuoso']['password'],
                                          auth_method: 'digest')
    @endpoint = 'http://localhost:8890/sparql'
    @evaluation = evaluation
    config
  end

  def run_query(endpoint, query)
    result = SPARQL::Client.new(endpoint).query(query)
    yield result if block_given?
    result
  end

  def get_triples(subject_uri, graph: 'merged')
    if subject_uri.nil?
      return nil
    end
    graph = 'mapped' if @evaluation==true
    subject = RDF::URI.new(subject_uri)
    query = RDF::Virtuoso::Query.select.where([subject, :p, :o]).to_s
    query.insert query.index('WHERE'),"FROM <#{@graphs[graph]}> "
    triples = Matcher::Triples.new(subject)

    solutions = run_query(@endpoint, query)
    solutions.each_solution do |solution|
      triples.add_p_o(solution.bindings)
    end
    triples
  end

  def get_entities_of_type(entity_type)
    uri_list = []
    if entity_type.nil?
      return []
    end
    query = "select distinct ?s from <#{@graphs['merged']}> where {?s ?p <#{entity_type.to_s}>.}"
    results = run_query(@endpoint, query)
    results.each_solution do |solution|
      uri_list << solution.bindings[:s]
    end
    return uri_list
  end

  def get_movie_subjects_by_imdb(imdb_id)
    s_list = []
    query = "select distinct ?s from <#{@graph}> where {
                    ?s <http://semmul2014.hpi.de/lodofmovies.owl#imdb_id> '#{imdb_id}'.
                    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <#{@types['movie_type']}>}"
    results = run_query(@endpoint, query)
    results.each_solution do |solution|
      s_list << solution.bindings[:s]
    end
    s_list
  end

  def get_movie_subjects_by_fb_mid(fb_mid)
    s_list = []
    query = "select  distinct ?s from <#{@graph}> where {?s ?p ?o.
                    ?s <http://semmul2014.hpi.de/lodofmovies.owl#freebase_mid> ?mid.
                    FILTER (REGEX(STR(?mid), '#{fb_mid.to_s}', 'i'))}"
    results = run_query(@endpoint, query)
    results.each_solution do |solution|
      s_list << solution.bindings[:s]
    end
    return s_list
  end

  def get_actor_triples(movie_triples)
    actor_triples = []
    performances = movie_triples.get_performances
    performances.each do |p|
      p_triples = get_triples p
      p_actor_uris = p_triples.get_actor
      p_actor_uris.each do |actor_uri|
        actor_triples << get_triples(actor_uri)
      end
    end
    return actor_triples
  end

  def get_same_as(entity_triples)
    same_as = []
    entity_uri = entity_triples.subject.to_s
    # get entities from main_db, that have a sameAs to the entity (which is from map_db)
    query = "select ?s from <#{@graph}> where {?s owl:sameAs <#{entity_uri}>}"
    results = run_query(@endpoint, query)
    results.each_solution do |solution|
      same_as << solution.bindings[:s]
    end

    return same_as
  end

  private

  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end

  def config
    matching = YAML.load_file '../config/matching.yml'
    @control = matching['control']
    namespaces = YAML.load_file '../config/namespaces.yml'
    @graphs = namespaces['graphs']
    @graph = @graphs['merged']
    @types = namespaces['types']
  end
end
