require_relative '../lib/dbpedia_mapper'

mapper = DBpediaMapper::Mapper.new
# mapper.register_receiver
# for each ID:
# 1. get (read) triple from Virtuoso
# 2. if mapping exists: map triple to own ontology
# 3. write triple to Virtuoso

v = DBpediaMapper::Virtuoso.new

subjects = v.get_all_subjects()

subjects.each_solution do |subject|
  s = subject.bindings[:s]
  values = v.get_all_for_subject(s)
  values.each_solution do |v|
    p = v.bindings[:p]
    o = v.bindings[:o]
    mapper.map(s, p, o)
  end
end