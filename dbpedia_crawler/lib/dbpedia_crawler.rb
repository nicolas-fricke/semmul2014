# encoding: utf-8

# Module DBpediaCrawler includes some classes for crawling RDF data from
# (a) DBpedia.
# 
# - Crawler: central class, utilizes other classes, executes commands
# - Fetcher: implements high-level fetching of data
# - Queue: provides handling of commands for the crawler
# - Source: provides low-level access to the data of a DBpedia
# - Writer: provides means of persisting data
#
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
  require_relative 'dbpedia_crawler/fetcher'
  require_relative 'dbpedia_crawler/queue'
  require_relative 'dbpedia_crawler/source'
  require_relative 'dbpedia_crawler/writer'

end
