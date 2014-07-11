Gem::Specification.new do |s|
  s.name        = 'semmul_tools'
  s.version     = '0.1.0'
  s.licenses    = ['MIT']
  s.summary     = 'Various tools for the Semmul 2014 Project'
  s.authors     = ['Johannes Jasper']
  s.email       = 'mail@johannesjasper.de'
  s.files       = `git ls-files`.split("\n")
  s.add_dependency 'bunny', '~>1.3.1'
  s.add_dependency 'rdf', '~>1.1.4.2'
  s.add_dependency 'rdf-virtuoso', '~>0.1', '>=0.1.6'
  s.add_dependency 'sparql-client', '~>1.1', '>=1.1.2'
  s.add_dependency 'rdf-json', '~>1.1', '>=1.1.0'
end