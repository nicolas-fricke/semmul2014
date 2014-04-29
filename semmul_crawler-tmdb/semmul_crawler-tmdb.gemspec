# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'semmul_crawler/tmdb/version'

Gem::Specification.new do |spec|
  spec.name          = 'semmul_crawler-tmdb'
  spec.version       = SemmulCrawler::Tmdb::VERSION
  spec.authors       = ['Nicolas Fricke']
  spec.email         = ['mail@nicolasfricke.de']
  spec.summary       = %q{Crawler for fetching movie information from themoviedb.org}
  spec.description   = %q{TODO: Write a longer description. Optional.}
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake'
end
