#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'

class VirtuosoExample
  def initialize
    secrets = YAML.load_file 'secrets.yml'
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: secrets['services']['virtuoso']['username'],
                                           password: secrets['services']['virtuoso']['password'],
                                           auth_method: 'digest')
  end

  def run_query(endpoint, query)
    result = SPARQL::Client.new(endpoint).query(query)
    yield result if block_given?
    result
  end

  def read_example
    #there are two ways of building the query
    # as object
    query = RDF::Virtuoso::Query.select.where([:s, :p, :o])
    # more complex example
    #graph = RDF::URI.new("http://www.w3.org/2002/07/owl#")
    #predicate = RDF::URI.new("http://www.w3.org/2000/01/rdf-schema#label")
    #query = RDF::Virtuoso::Query.select.where([:s, predicate, :o]).graph(graph)

    #or as string
    query_2 = 'select * where {?s ?p ?o}'

    endpoint = 'http://localhost:8890/sparql'
    #endpoint = "http://dbpedia.org/sparql"

    run_query(endpoint, query).each_solution do |solution|
      p solution.bindings
    end

  end


  def write_example
    graph = RDF::URI.new('http://test.com')

    subject = RDF::URI.new('http://subject')
    predicate = RDF::URI.new('http://predicate')

    query = RDF::Virtuoso::Query.insert([subject, predicate, 'literal_object']).graph(graph)
    p @repo.insert(query)
  end
end

v = VirtuosoExample.new
#v.write_example
v.read_example
