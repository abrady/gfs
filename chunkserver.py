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
import gfs
import net
import master
import io
import fnmatch
import re
import hashlib
import log


chunkservid = 0

def log(str):
	log.log("[chunk%i] %s" % (chunkserverid,str))


class ReadMsg:
	"object sent from clients for read requests"
	def __init__(self,id,offset,len):
		self.id = id
		self.offset = offset
		self.len = len


class ChunkInfo:
	"""info about a particular chunk that a chunkserver owns:
	- ids
	- set of checksums
	"""
	def __init__(self, id, checksum):
		self.id = id
		self.checksum = checksum

	def __init__(self,id):
		f = open(id + '.chunk','rb')
		s = f.read(settings.CHUNK_SIZE)
		m = hashlib.md5()
		m.update(s)
		self.__init__(id,m.digest())

		
class Meta:
	'all the info about the chunks that a chunkserver owns'
	def __init__(self):
		self.chunks = {}

	def add_existing_chunk(self,chunkid):
		"""add an existing chunk to a chunkserver by id.
		Assumes file already exists"""
		if self.chunks.has_key(chunkid):
			sys.stderr.write("Error: re-adding chunk " + chunkid + "ignoring")
			return
		self.chunks[chunkid] = ChunkInfo(chunkid)


def load_chunkinfo():
	'load up all the chunks in the local fs'

	# TODO: cache the meta info.
	# right now just rebuild it each time
	meta = Meta()	
	chunkfiles = fnmatch.filter(os.listdir('.'),"*.chunk")
	for cf in chunkfiles:
		id = re.sub(".chunk","",cf)
		if not chunkinfo.chunks.has_key(id):
			meta.add_existing_chunk(id)
	return meta

def srv():
	"""main function for a chunkserver:
	- scan the chunkdir for chunks
	"""
	chunkservid += 1
	chunkdir = settings.CHUNK_DIR + str(chunkserverid)
	chunkserverid += 1
	os.mkdir(chunkdir)
	os.chdir(chunkdir)
	
	meta = load_chunkinfo()
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((settings.MASTER_ADD,R,settings. MASTER_CHUNKPORT))
	master_conn = net.PakSender(s)
	master_conn.send_obj(ChunkConnectMsg(meta.chunks.keys))



if __name__ == "__main__":
	chunkserver()
