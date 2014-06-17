# encoding: utf-8

# Module DBpediaCrawler includes some classes for crawling RDF data from
# (a) DBpedia.
# 
# - Crawler: central class, run one for each DBpedia, uses the other classes
# - Queue: provides handling of commands for the crawler
# - Source: provides access to the data of DBpedia
# - Writer: provides means of persisting crawled data
#
# TODO: add "retry" parameter to failed commands pushed to the queue again
# TODO: query related movie data
# TODO: query related show data
# TODO: remove false positives (e.g. "1960 in film"), especially due to categories
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
