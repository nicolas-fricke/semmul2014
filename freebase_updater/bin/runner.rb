require_relative '../lib/freebase_updater'

movie_updater = FreebaseUpdater::Updater.new
puts movie_updater.register_receiver
