TESTING = 1
DEBUG = True
BLOCK_SIZE = 64000 # in orignal paper 64MB, but this is better for testing
CHUNKSERVERS = {
	'A' : {},
	'B' : {},
	'C' : {},
	}

MASTER_ADDR = 'localhost'
MASTER_CHUNKPORT = 50000
MASTER_CLIENTPORT = 50007

MAX_MASTERCMD_SIZE = 16384 # totally arbitrary, but recvs will fail if objects sent over the wire are larger than this 
