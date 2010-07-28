#TODO logging
"""
Metadata:
full pathnames to metadata (with prefix compression)
- no 'inode' type hierarchy of data
- no symbolic names or aliases

Master Operations:

snapshot: a snapshot of a subset of the file system can be made by a client by request. the master revokes all leases and marks all chunks as needing a copy on write. When a chunk is finally written to and it is marked as such each chunkserver is first asked to make a copy of the chunk, and then the new chunk's handle is returned.

locks are aquired in 'directory' order, e.g. for d1/d2/.../dn read locks will be acquired d1, d2, ..., dn, and a write lock acquired on d1/d2/.../dn/leaf.

For snapshots, say /home/user is being snapshotted to /save/user. /home/user/foo is prevented from being created because /home/user gets a write lock which prevents a read lock on the same path. i.e.
- file creation only requires a read lock on the directory
- snapshots require write locks on the directory

so the operations are serialized properly.

(Note: I didn't see any info about timeouts or queueing of operations. I'm assuming there is a queue with a long timeout for these things as a snapshot might never succeed in acquiring a write lock for a directory otherwise)

"""
import sys
import socket
import select 
import cPickle
import net
from log import log

class ChunkConnect:
	"message from a chunkserver when it first connects"

	def __init__(self,ids):
		'inits this message with all the UIDs for blocks a chunkserver knows'
		self.ids = ids
		
	def __call__(self,meta):
		log "ChunkConnect"
		
		

def srv(settings):
	
	chunkservers = {}	
			
	sock_chunk = net.listen_sock(settings.MASTER_CHUNKPORT)
	sock_client = net.listen_sock(settings.MASTER_CLIENTPORT)

	# select.select

	def handle_clients(s):
		try:
			conn, addr = s.accept()
		except socket.error:
			return # no conn, done
		
		log('client conn from %s' % str(addr))
		# TODO client metadata requests
		conn.close()
		return

	def handle_chunkservers(s, cs):
		try:
			conn, addr = s.accept()
		except socket.error:
			return # no conn, done
		# new connection
		log('chunkserv conn from %s' % str( addr))

		if(cs.has_key(addr)):
			sys.stderr.write("duplicate address %r, dropping old" % addr)
		cs[addr] = conn

		rds,_,_ = select.select(cs.values(),(),())
		
		# chunks are trusted, just read the objects and execute them
		for r in rds:
			recvr = net.Receiver(r)
			o = recvr.recv_obj()
			log("recv " + str( o))
			o()
			
			#debug
			sock_client.close()
			sock_chunk.close()
			sys.exit()
			
		
	while 1:
		handle_clients(sock_client)
		handle_chunkservers(sock_chunk, chunkservers)


