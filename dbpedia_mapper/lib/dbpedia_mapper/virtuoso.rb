#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'

class DBpediaMapper::Virtuoso
  def initialize
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: secrets['services']['virtuoso']['username'],
                                           password: secrets['services']['virtuoso']['password'],
                                           auth_method: 'digest')
  end

  def write_mapped(subject, predicate, object, graph='http://example.com/mapped')
    graph = RDF::URI.new(graph)
    subject = RDF::URI.new(subject)
    predicate = RDF::URI.new(predicate)

    query = RDF::Virtuoso::Query.insert([subject, predicate, object]).graph(graph)
    p @repo.insert(query)
  end

  def run_query(endpoint, query)
    result = SPARQL::Client.new(endpoint).query(query)
    yield result if block_given?
    result
  end

  def read_all()
    graph = 'http://example.com/raw/'
    query = "select distinct * from <#{graph}> where {?s ?p ?o}"

    endpoint = 'http://localhost:8890/sparql'
    return run_query(endpoint, query)
  end

  def get_all_subjects()
    graph = 'http://example.com/raw/'
    query = "select distinct ?s from <#{graph}> where {?s ?p ?o}"

    endpoint = 'http://localhost:8890/sparql'
    return run_query(endpoint, query)
  end

  def get_all_for_subject(subject)
    graph = 'http://example.com/raw/'
    query = "select distinct ?p ?o from <#{graph}> where {<#{subject}> ?p ?o}"

    endpoint = 'http://localhost:8890/sparql'
    return run_query(endpoint, query)
  end

  private
  def secrets
    @secrets ||= YAML.load_file '../config/secrets.yml'
  end
end