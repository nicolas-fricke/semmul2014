#!/bin/sh
CSV_PATH=$PWD/movie_links.csv

DIRS="../dbpedia_crawler
../freebase_updater/
../tmdb_updater/"

for DIR in $DIRS; do
    cd $DIR
    echo $PWD
    bundle exec ruby bin/demo.rb $CSV_PATH
done
