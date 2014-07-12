module Matcher
    require_relative 'matcher/matcher'
    require_relative 'matcher/virtuoso'
    require_relative 'matcher/triples'
end


if __FILE__ == $0

	m = Matcher::Matcher.new()
    found = m.find("http://dbpedia.org/resource/$9.99")
    if found.nil?
        puts "nothing found"
    else
        puts "found #{found}"
    end

end