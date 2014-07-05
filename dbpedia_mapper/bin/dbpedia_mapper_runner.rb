require_relative '../lib/dbpedia_mapper'
require_relative '../lib/dbpedia_mapper/virtuoso'

mapper = DBpediaMapper::Mapper.new
# mapper.register_receiver
# for each ID:
# 1. get (read) triple from Virtuoso
# 2. if mapping exists: map triple to own ontology
# 3. write triple to Virtuoso

# v = DBpediaMapper::Virtuoso.new

# subjects = v.get_all_subjects()

# subjects.each_solution do |subject|
#   s = subject.bindings[:s]
#   mapper.map_entity(s, true)
# end

mapper.map_entity("http://dbpedia.org/resource/!Women_Art_Revolution", true)
# mapper.map_entity("http://dbpedia.org/resource/!Hero", true)