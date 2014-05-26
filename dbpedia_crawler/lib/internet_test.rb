require_relative "dbpedia_crawler"

#
# Just some code which uses dbpedia_crawler.rb,
# assuming that there is an internet connection and
# DBpedia actually works.
#

# query triples about Sean Connery
puts DbpediaCrawler.new.triples_of_entity "http://dbpedia.org/resource/Sean_Connery"

# query all ids (limited to 100)
puts DbpediaCrawler.new.query_all_ids
