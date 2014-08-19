require 'csv'
require_relative '../../general/virtuoso_reader'
require_relative '../lib/matcher'

def evaluate(path)
  p path
  counter = 1
  reader = VirtuosoReader.new graph:'mapped'
  matcher = Matcher::Matcher.new(true)

  matching ||= YAML.load_file '../config/matching.yml'
  thresholds = matching['thresholds']
  weights = matching['weights']

  matches_db_fb = []
  matches_db_tmdb = []
  matches_fb_tmdb = []

  distractor_matches_db_fb = []
  distractor_matches_db_tmdb = []
  distractor_matches_fb_tmdb = []

  last_db = nil
  last_fb = nil
  last_tmdb = nil

  db_fb   = {tp: 0, fp:0, tn: 0, fn:0}
  db_tmdb = {tp: 0, fp:0, tn: 0, fn:0}
  tmdb_fb = {tp: 0, fp:0, tn: 0, fn:0}

  CSV.foreach(path, { :col_sep => "\t" }) do |dbpedia_link, fb_link, tmdb_link|

    begin

      # subject uris
      dbpedia_exists = reader.exists_subject subject: dbpedia_link
      fb_mid = fb_link.split("freebase.com/m/")[1]
      fb_uri = "http://rdf.freebase.com/ns/m/#{fb_mid}"
      fb_exists = reader.exists_subject subject: fb_uri
      tmdb_id = tmdb_link.split("themoviedb.org/")[1].split("/")[1][/\d+/]
      tmdb_uri = "http://semmul2014.hpi.de/tmdb/movie/#{tmdb_id}"
      tmdb_exists = reader.exists_subject subject: tmdb_uri

      # db-fb match
      if dbpedia_exists and fb_exists
        match01 = matcher.evaluation_match(dbpedia_link, fb_uri)
        matches_db_fb << match01

        # db-fb distractor match
        use_last_db = [true, false].sample
        if use_last_db
          unless last_db.nil?
            d_match01 = matcher.evaluation_match(last_db, fb_uri)
            distractor_matches_db_fb << d_match01
          end
        else
          unless last_fb.nil?
            d_match01 = matcher.evaluation_match(dbpedia_link, last_fb)
            distractor_matches_db_fb << d_match01
          end
        end


      else
        matches_db_fb << nil
        distractor_matches_db_fb << nil
      end

      # db-tmdb match
      if dbpedia_exists and tmdb_exists
        match02 = matcher.evaluation_match(dbpedia_link, tmdb_uri)
        matches_db_tmdb << match02

        # db-tmdb distractor match
        use_last_db = [true, false].sample
        if use_last_db
          unless last_db.nil?
            d_match02 = matcher.evaluation_match(last_db, tmdb_uri)
            distractor_matches_db_tmdb << d_match02
          end
        else
          unless last_tmdb.nil?
            d_match02 = matcher.evaluation_match(dbpedia_link, last_tmdb)
            distractor_matches_db_tmdb << d_match02
          end
        end

        last_db = dbpedia_link
        last_tmdb = tmdb_uri
      else
        matches_db_tmdb << nil
        distractor_matches_db_tmdb << nil
      end

      # fb-tmdb match
      if fb_exists and tmdb_exists
        match12 = matcher.evaluation_match(fb_uri, tmdb_uri)
        matches_fb_tmdb << match12

        # fb-tmdb distractor match
        use_last_fb = [true, false].sample
        if use_last_fb
          unless last_fb.nil?
            d_match12 = matcher.evaluation_match(last_fb, tmdb_uri)
            distractor_matches_fb_tmdb << d_match12
          end
        else
          unless last_tmdb.nil?
            d_match12 = matcher.evaluation_match(fb_uri, last_tmdb)
            distractor_matches_fb_tmdb << d_match12
          end
        end

      else
        matches_fb_tmdb << nil
        distractor_matches_fb_tmdb << nil
      end

      last_db = dbpedia_link
      last_fb = fb_uri
      last_tmdb = tmdb_uri


    rescue Exception => e
      puts e
    end
    $stdout.write "\r#{counter}/1000"
    $stdout.flush
    counter += 1
    #break if counter >= 100
  end
  puts ""

  db_fb = count_matches(matches_db_fb, db_fb, thresholds)
  db_fb = count_distractor_matches(distractor_matches_db_fb, db_fb, thresholds)
  eval_db_fb = calculate_eval(db_fb)

  db_tmdb = count_matches(matches_db_tmdb, db_tmdb, thresholds)
  db_tmdb = count_distractor_matches(distractor_matches_db_tmdb, db_tmdb, thresholds)
  eval_db_tmdb = calculate_eval(db_tmdb)

  tmdb_fb = count_matches(matches_fb_tmdb, tmdb_fb, thresholds)
  tmdb_fb = count_distractor_matches(distractor_matches_fb_tmdb, tmdb_fb, thresholds)
  eval_tmdb_fb = calculate_eval(tmdb_fb)

  puts "DBpedia - Freebase:"
  puts db_fb
  puts eval_db_fb
  puts "  accuracy:  #{eval_db_fb[:accuracy]}"
  puts "  precision: #{eval_db_fb[:precision]}"
  puts "  recall:    #{eval_db_fb[:recall]}"
  puts "  f-measure: #{eval_db_fb[:fmeasure]}"

  puts "DBpedia - TMDB:"
  puts db_tmdb
  puts eval_db_tmdb
  puts "  accuracy:  #{eval_db_tmdb[:accuracy]}"
  puts "  precision: #{eval_db_tmdb[:precision]}"
  puts "  recall:    #{eval_db_tmdb[:recall]}"
  puts "  f-measure: #{eval_db_tmdb[:fmeasure]}"

  puts "Freebase - TMDB:"
  puts tmdb_fb
  puts eval_tmdb_fb
  puts "  accuracy:  #{eval_tmdb_fb[:accuracy]}"
  puts "  precision: #{eval_tmdb_fb[:precision]}"
  puts "  recall:    #{eval_tmdb_fb[:recall]}"
  puts "  f-measure: #{eval_tmdb_fb[:fmeasure]}"

  puts "----------------------------------------"
  puts "Average:"
  puts "  accuracy:  #{(eval_db_fb[:accuracy]  + eval_tmdb_fb[:accuracy]  + eval_db_tmdb[:accuracy])/3}"
  puts "  precision: #{(eval_db_fb[:precision] + eval_tmdb_fb[:precision] + eval_db_tmdb[:precision])/3}"
  puts "  recall:    #{(eval_db_fb[:recall]    + eval_tmdb_fb[:recall]    + eval_db_tmdb[:recall])/3}"
  puts "  f-measure: #{(eval_db_fb[:fmeasure]  + eval_tmdb_fb[:fmeasure]  + eval_db_tmdb[:fmeasure])/3}"

  puts ""
  puts weights
  puts thresholds

end

DEFAULT_PATH = '../demo/1000_Movies.tsv'


def count_matches(matches, data, thresholds)
  matches.each do |match|
    unless match.nil?
      if match > thresholds['matching']
        data[:tp] += 1
      else
        data[:fn] += 1
      end
    end
  end
  data
end

def count_distractor_matches(matches, data, thresholds)
  matches.each do |d_match|
    unless d_match.nil?
      if d_match > thresholds['matching']
        data[:fp] += 1
      else
        data[:tn] += 1
      end
    end
  end
  data
end

def calculate_eval(data)
  accuracy = (data[:tp] + data[:tn]).to_f / (data[:tp] + data[:tn] + data[:fp] + data[:fn]).to_f
  precision = data[:tp] / (data[:tp] + data[:fp]).to_f
  recall = data[:tp] / (data[:tp] + data[:fn]).to_f
  beta = 0.5
  fmeasure = (1+(beta*beta)) * (precision * recall) / ((beta * beta * precision) + recall)
  return {accuracy:accuracy, precision:precision, recall:recall, fmeasure:fmeasure}
end

evaluate(File.absolute_path DEFAULT_PATH)
