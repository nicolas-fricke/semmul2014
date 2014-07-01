require_relative '../lib/tmdb_updater'

updater = TMDbUpdater::Updater.new
updater.register_receiver