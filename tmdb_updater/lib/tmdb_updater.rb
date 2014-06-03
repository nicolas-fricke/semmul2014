# require_relative 'tmdb_updater'
require_relative 'tmdb_updater/msg_receiver'
# require_relative 'tmdb_updater/msg_publisher'

receiver = MsgReceiver.new
receiver.subscribe :movie_id do |body|
  puts "received: #{body}"
  sleep 2
end
