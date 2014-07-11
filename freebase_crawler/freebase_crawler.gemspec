Gem::Specification.new do |s|
  s.name        = 'freebase_crawler'
  s.version     = '0.1.0'
  s.licenses    = ['MIT']
  s.summary     = "Crawler for Freebase APIs"
  s.authors     = ["Johannes Jasoer"]
  s.email       = 'mail@johannesjasper.de'
  s.files       = ["lib/freebase_crawler.rb"]

  s.add_dependency 'httparty', '~>0.12', '>=0.12.0'
  s.add_dependency 'addressable', '~>2.3', '>=2.3.6'
end
