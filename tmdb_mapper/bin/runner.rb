require_relative '../lib/tmdb_mapper'

mapper = TMDbMapper::Mapper.new
mapper.register_receiver
# for each ID:
# 1. get (read) triple from Virtuoso
# 2. if mapping exists: map triple to own ontology
# 3. write triple to Virtuoso