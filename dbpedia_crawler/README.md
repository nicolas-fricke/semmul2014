# DBpedia Crawler

For crawling DBpedia data (RDF).

## Running several crawlers for one DBpedia

The crawler can be run several times (as independent processes) to achieve a
greater throughput of commands:

First crawler (working directory does not matter, adjust the path as necessary):

    ruby bin/dbpedia_crawler.rb

With default options, the first crawler purges the queue, pushes the initial "crawl all IDs" command on the queue,
and when there are no commands left anymore, it pushes the "crawl all IDs" command again.

Further crawlers ("do not crawl ids on start / do not crawl ids when no command is left / do not purge the queue"):

    ruby bin/dbpedia_crawler.rb -crawler:crawl_all_ids:false -crawler:insomnia:false -queue:purge:false

This crawler only handles commands which are on the queue, but will not push a "crawl all IDs" command (if there 
are no commands left, it will sleep).

## Crawling another DBpedia

By default, the crawler crawls data from "http://dbpedia.org/sparql".
To crawl data from another DBpedia, start the crawler with the respective options, e.g.:

    ruby bin/dbpedia_crawler.rb -queue:agent_id:"dbpedia_live" -source:endpoint:"http://live.dbpedia.org/sparql"

This crawler uses "dbpedia_live" for its queues (important, otherwise the commands from different crawlers will
be pushed to the same queues!) and sends SPARQL queries to "http://live.dbpedia.org/sparql". Note: if the endpoint
includes "live.dbpedia.org", attempts to access linked data via content negotiation will be directed to
"http://live.dbpedia.org/..." instead of "http://dbpedia.org" (this is necessary because DBpedia Live does not use
language-specific prefixes for its entities like the other DBpedias, except the default English one, do).

Note that it is not possible right now to specify which SPARQL queries to use, so if an endpoint cannot deal
with the queries which are used by the crawler, crawling may not work (e.g. property paths).

## Type Checking

Type checking for crawled movies and shows can be activated by setting the options "crawler:check_types"
and "fetcher:threshold" accordingly. 

The type checker uses the DBpedia Type Completion Service,
http://wifo5-21.informatik.uni-mannheim.de:8080/DBpediaTypeCompletionService/.
See Paulheim and Bizer: Type Inference on Noisy RDF Data. In: International Semantic Web Conference (ISWC), 2013

As of now, this is deactivated by default, as the REXML parser reports problems with the encoding of 
the returned XML document.

## Using Bunny

See https://github.com/ruby-amqp/bunny.

You need a running rabbitmq server (after installing the package on a Unix system, 
it should be running):

    sudo apt-get install rabbitmq-server

If ever necessary, restart it with:

    sudo /etc/init.d/rabbitmq-server restart

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

## Using RDF::Virtuoso

See https://github.com/digibib/rdf-virtuoso/.

Requires to install the following gem (or use bundle install):

    sudo gem install sparql 
    sudo gem install rdf-virtuoso 

