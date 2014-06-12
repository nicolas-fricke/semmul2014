# encoding: utf-8

# Module DBpediaCrawler includes some classes for crawling RDF data from
# (a) DBpedia.
# 
# - Crawler: central class, create one for each DBpedia, uses the other classes
# - Queue: provides handling of commands for the crawler
# - Source: provides access to the data of DBpedia
# - Writer: provides means of persisting crawled data
#
# TODO: single source file for module which requires class files
# TODO: query all movie IDs
# TODO: query all relevant IDs (movies + shows)
# TODO: query related entity data and add it to the data store
# TODO: search for updates
# 
# TODO: try with other DBpedias
# TODO: check bunny options
# TODO: interface of bunny queues / commands
# TODO: validate configuration options
# TODO: logging
# TODO: testing :)
module DBpediaCrawler

  require_relative 'dbpedia_crawler/crawler'
  require_relative 'dbpedia_crawler/queue'
  require_relative 'dbpedia_crawler/source'
  require_relative 'dbpedia_crawler/writer'

end
