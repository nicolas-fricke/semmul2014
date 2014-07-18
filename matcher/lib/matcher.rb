module Matcher
    require_relative 'matcher/matcher'
    require_relative 'matcher/virtuoso'
    require_relative 'matcher/triples'
    require relative 'matcher/evaluation'
end


if __FILE__ == $0

	m = Matcher::Matcher.new()
    found = m.find("http://rdf.freebase.com/ns/m/0hhzyzv")
    if found.nil?
        puts "nothing found"
    else
        puts "found #{found}"
    end

end