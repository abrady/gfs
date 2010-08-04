"""
In the GFS, the chunkserver manages blocks (chunks) of a fixed size (64MB in GFS, but I'll probably do something smaller for ease of testing).

reliability is acheived by having a collection of chunkservers have information about the same block (by default each chunk is replicted on three machines, though more copies may be made for high demand data).

For performance reasons, the master elects one of the owners of a chunk as a 'primary' to handle the ordering of operations to the replicas.

When writing to a chunk, the data is written before the write is committed, in this way the data write can optimize network topology. For example, a client writing to S1, S2, S3, S4 may have S1 closest, even though S4 is the primrary. The writes its data to S1, which forwards data to S2, S2 forwards to S3, which forwards to S4. This forwarding can begin even before the client's write finishes.

If a write crosses a chunk boundary, the chunkserver rejects the write, marks the chunk as full, and asks the client to request a new chunk. The maximum write size is restricted to a quarter of a chunk, so fragmentation is low.

Atomic Record Appends
___________________

Chunkservers are updated through atomic appends where a client requests not where data goes, but only that it goes at 'the end' of the chunk.

The primary defines the order that the record appends occur, and this is what keeps the chunks looking nearly identical on all replicas.


Mutation Order
____________


A mutation is a chance of chunk contents or metadata. Each mutation is performed at all the chunk's replicas.

Mutations are handled on a chunkserver that is granted a temporary 'lease' from the master, which becomes responsible for ordering concurrent mutations on a chunk. (Note: the only purpose of the lease is to ease load on the master.)

Mutation Steps:

1) client requests chunk.

2) master sends client list of chunkservers including primary (granting a primary chunkserver a lease for the chunk if none currently held)

3) client pushes data to closest chunkserver (which replicates data to its closest replica, etc. This data is stored in memory(?) in an LRU cache)

4) once all replicas have ack'd receiving the data, client sends a 'commit' write request to the primary. The primary picks an order of operation and applies the write to its own disk.

5) The primary forwards write requests to all secondary chunkservers. All chunkservers perform the write requests in the order specified by the primary

6) secondaries reply to the primary that they have completed the operatoin

7) Primary replies to the client with success.
- In the case of partial or full failure (i.e. some chunkserver failed to write), the client considers this a failure and retries steps 3 through 7 until eventually retrying the entire process.

"""

import settings
import socket
import net
import io
import fnmatch
import re
import hashlib
import os
import cPickle

import log
import msg

try:
	import settings # Assumed to be in the same directory.
except ImportError:
	sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. This is required\n" % __file__)
	sys.exit(1)
	
	if(settings.DEBUG):
		reload(settings)



chunkservid = 0

def checksum_chunk(fn):
	f = open(fn,'rb')
	s = f.read(settings.CHUNK_SIZE)
	m = hashlib.md5()
	m.update(s)
	return m.digest()
	

class ChunkInfo:
	"""info about a particular chunk that a chunkserver owns:
	- ids
	- set of checksums
	"""
	def __init__(self, id, checksum):
		self.id = id
		self.checksum = checksum

class ChunkServer:
	"class for managing a chunkserver"
	def __init__(self):
		"""inits a chunkserver:
		- scan the chunkdir for chunks
		- connect to master
		- open client listening port
		"""
		global chunkservid
		self.id = chunkservid
		self.log("chunkserver init")
		
		self.chunkdir = settings.CHUNK_DIR + str(chunkservid)		
		chunkservid += 1
		
		if not os.path.exists(self.chunkdir):
			os.mkdir(self.chunkdir)
		self._load()
	
		s = net.client_sock(settings.MASTER_ADDR,settings. MASTER_CHUNK_PORT)
		self.master = net.PakComm(s)
		chunk_conn = msg.ChunkConnect(s.getsockname(),self.chunks.keys())
		self.master.send_obj(chunk_conn)

		s = net.listen_sock(settings.CHUNK_CLIENT_PORT)
		self.client_server = net.PakServer(s,self.chunkdir)

	def tick(self):
		"function for chunkserver to send and receive requests"
		def client_req_handler(obj,sock):
			self.log("obj %s from client %s" % (obj,sock))
			obj(self,sock)
		self.client_server.tick(client_req_handler)

	def _load(self):
		'load up all the chunks in the chunks directory'
		
		# TODO: cache the checksum info.
		# right now just rebuild it each time
		chunkfiles = fnmatch.filter(os.listdir(self.chunkdir),"*chunk")
		self.log("loading meta for dir %s, %i chunks" % (self.chunkdir,len(chunkfiles)))

		self.chunks = {}
		for cf in chunkfiles:
			self.log("adding: " + cf)
			id = re.sub(".chunk","",cf)
			cs = checksum_chunk(os.path.join(self.chunkdir,cf))
			self.chunks[id] = ChunkInfo(id,cs)

	def log(self,str):
		log.log("[chunk%i] %s" % (self.id,str))

	def write_test_chunk(self):
		f = open(os.path.join(self.chunkdir,"0.chunk"),"wb")
		s = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_='
		for i in range(settings.CHUNK_SIZE/len(s)):
			f.write(s)
		f.close()

	
