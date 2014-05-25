# DBpedia Crawler

For crawling DBpedia data (RDF).

## Using RDF.rb

See http://ruby-rdf.github.io/rdf/ and https://github.com/ruby-rdf/rdf.

Licence: public domain.

Includes query mechanisms, adapters for several storage systems
as well as several data formats (RDF/XML, Turtle et cetera).

Install the linkeddata gem, which incorporates the core gems (this takes a while,
like several minutes):

  sudo gem install linkeddata 

If you get an error like 

  custom_require.rb:36:in `require': cannot load such file -- mkmf

you have to install the package ruby-dev first (which is necessary for 
building Ruby extensions):

  sudo apt-get install ruby-dev

## Using rdf-virtuoso

See https://github.com/digibib/rdf-virtuoso.

Includes an adapter for Virtuoso based on RDF.rb.

TODO: use dat stuff and document its usage

