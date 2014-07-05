# encoding: utf-8

# Module DBpediaCrawler includes some classes for crawling RDF data from
# (a) DBpedia.
# 
# - Crawler: central class, utilizes other classes, executes commands
# - Fetcher: implements high-level fetching of data
# - Queue: provides handling of commands for the crawler
# - Source: provides low-level access to the data of a DBpedia
# - TypeChecker: checks types using type inference
# - Writer: provides means of persisting data
#
# TODO: improve and use type checking
# TODO: query related show data (allow to name queries in fetching rules)
module DBpediaCrawler

  require_relative 'dbpedia_crawler/crawler'
  require_relative 'dbpedia_crawler/fetcher'
  require_relative 'dbpedia_crawler/queue'
  require_relative 'dbpedia_crawler/source'
  require_relative 'dbpedia_crawler/type_checker'
  require_relative 'dbpedia_crawler/writer'

  require_relative '../../general/virtuoso_writer'

end
