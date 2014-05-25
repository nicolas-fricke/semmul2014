#!/usr/bin/env ruby
require 'rubygems'
require 'sparql/client'

VIRTUOSO_SPARQL_ENDPOINT = "http://localhost:8890/sparql"
DBPEDIA_SPARQL_ENDPOINT = "http://dbpedia.org/sparql"

def query_sparql(end_point, query)
  SPARQL::Client.new(end_point).query(query)
end

def query_virtuoso(query)
  result = query_sparql(VIRTUOSO_SPARQL_ENDPOINT, query)
  yield result if block_given?
end

query = "SELECT * FROM <urn:owl:inference:tests> WHERE { ?s ?p ?o . }"
query_virtuoso query do |results|
  p results
end
