require_relative '../lib/tmdb_updater'

updater = TMDbUpdater::Updater.new
updater.init_virtuoso
updater.register_receiver