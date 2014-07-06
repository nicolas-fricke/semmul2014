module Matcher
    require_relative 'matcher/matcher'
    require_relative 'matcher/virtuoso'
    require_relative 'matcher/triples'
end


if __FILE__ == $0

	m = Matcher::Matcher.new()
    found = m.find("http://rdf.freebase.com/ns/m/03lx3k")
    puts found

end