TESTING = 1
DEBUG = True
CHUNK_SIZE = 2**20 # in orignal paper 64MB, but this is better for testing
MAX_CHUNK_MUTATION_SIZE = CHUNK_SIZE/4 # keeps fragmentation low
CHUNK_DIR = "chunkdir"
CHUNK_CLIENT_PORT = 50010

MASTER_ADDR = 'localhost'
MASTER_CLIENT_PORT = 50007
MASTER_CHUNK_PORT = 50000

