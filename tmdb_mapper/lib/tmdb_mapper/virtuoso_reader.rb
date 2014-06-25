#!/usr/bin/env ruby
require 'yaml'
require 'sparql/client'
require 'rdf'
require 'rdf/virtuoso'

class TMDbMapper::VirtuosoReader
  def initialize
    @repo = RDF::Virtuoso::Repository.new('http://localhost:8890/sparql',
                                           update_uri: 'http://localhost:8890/sparql-auth',
                                           username: secrets['databases']['virtuoso']['username'],
                                           password: secrets['databases']['virtuoso']['password'],
                                           auth_method: 'digest')
  end

  def get_objects_for(subject:, predicate:, graph: 'http://example.com/mapped')
    # TODO: get and return objects from Virtuoso
  end

  private
  def secrets
    @secrets ||= YAML.load_file 'config/secrets.yml'
  end
end