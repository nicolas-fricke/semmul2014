require_relative '../lib/tmdb_updater'
require 'profiler'

updater = TMDbUpdater::Updater.new
updater.register_receiver