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
import log

def log(str):
	log.log("[master] " + str)

class ReadErr:
	def __init__(self):
		self.errmsg = "ReadErr"
	

class FileNotFoundErr(ReadErr):
	def __init__(self,errmsg):
		self.errmsg = errmsg


class ChunkInfo:
	"contains the id of a chunk, and the servers that manage it"
	def __init__(self,chunk_id,servers):
		self.id = chunk_id
		self.servers = servers

class ChunkConnectMsg:
	"message from a chunkserver when it first connects"

	def __init__(self,ids):
		'inits this message with all the UIDs for blocks a chunkserver knows'
		self.ids = ids
		
	def __call__(self,meta):
		log("ChunkConnectMsg")


class ClientReadMsg:
	"message from a clientserver when it first connects"

	def __init__(self,fname,chunk_index,len):
		'inits this message with a read request'
		self.fname = fname
		self.offset = offset
		self.len = len
		
	def __call__(self,sock,meta):
		log("ClientRead(%s,%i,len=%i"%(self.fname,self.offset,self.len))

		# get the file info
		file_info = meta.fileinfos[self.fname]
		if not file_info:
			sender = PakSender(sock)
			sender.send_obj(ReadErr("file '%s' not found" % self.fname))
			return

		# get the chunk info
		chunk_info = file_info.chunks[chunk_index]
		if not chunk_info:
			sender.send_obj(ReadErr("chunk '%i' not found"%chunk_index))
			return

		# queue up the read request. Because a write could be in operation
		# this could fail here.
		meta.queue[(self.fname,chunk_index)] = (sock, 'read')
		
		
		

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
			recvr = net.PakReceiver(r)
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


