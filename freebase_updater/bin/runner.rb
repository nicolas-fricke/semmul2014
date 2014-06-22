# Updater
require_relative '../lib/freebase_updater'

movie_updater = FreebaseUpdater::Updater.new
movie_updater.register_receiver
