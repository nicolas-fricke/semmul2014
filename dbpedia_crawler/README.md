# DBpedia Crawler

For crawling DBpedia data (RDF).

## Running several crawlers

The crawler can be run several times (as independent processes) to achieve a
greater throughput of commands:

First crawler (working directory does not matter, adjust the path as necessary):

	ruby bin/dbpedia_crawler.rb

Further crawlers ("do not push the initial command / do not purge the queue"):

	ruby bin/dbpedia_crawler.rb -crawler:crawl_all_ids:false -queue:purge:false

## Using Bunny

See https://github.com/ruby-amqp/bunny.

You need a running rabbitmq server (after installing the package on a Unix system, 
it should be running):

	sudo apt-get install rabbitmq-server

Install the bunny gem (or use bundle install):

	sudo gem install bunny

## Using RDF.rb

See http://ruby-rdf.github.io/rdf/ and https://github.com/ruby-rdf/rdf.

Licence: public domain.

Includes query mechanisms, adapters for several storage systems
as well as several data formats (RDF/XML, Turtle et cetera).

Install the linkeddata gem, which incorporates the core gems (this takes a while,
like several minutes) (or use bundle install):

	sudo gem install linkeddata 

If you get an error like 

	custom_require.rb:36:in `require': cannot load such file -- mkmf

you have to install the package ruby-dev first (which is necessary for 
building Ruby extensions):

	sudo apt-get install ruby-dev

## Using RDF::Client with Virtuoso

For Virtuoso to accept SPARQL Updates, the user SPARQL must belong to the group SPARQL_UPDATE
(System Admin => User Accounts => Edit user SPARQL).

