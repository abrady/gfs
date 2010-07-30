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
import time

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
		self.chunk_index = chunk_index
		self.len = len
		
	def __call__(self,sock,meta):
		'fulfill the request if nothing is queued'
		log("ClientReadQ(%s,%i,len=%i"%(self.fname,self.offset,self.len))
		# queue up the read request. Because a write could be in operation
		# this could fail here.
		if not meta.client_req_queues[(self.fname,self.chunk_index)]:
			meta.client_req_queues[(self.fname,self.chunk_index)] = []
		meta.queue[(self.fname,self.chunk_index)].append((self,sock, 'read'))

	def fulfull(self,meta,sock):
		"""dispatch the call when it reaches the front of the
		queue. returns True if this request should be removed from the
		queue, False otherwise"""
		
		# get the file info
		file_info = meta.fileinfos[self.fname]
		if not file_info:
			sender = PakSender(sock)
			sender.send_obj(ReadErr("file '%s' not found" % self.fname))
			return True

		# get the chunk info
		chunk_info = file_info.chunks[chunk_index]
		if not chunk_info:
			sender.send_obj(ReadErr("chunk '%i' not found"%chunk_index))
			return True

		if chunk_info.lock == 'write':
			return False # wait until write done to do a read

		chunk_info.lock = 'read'
		chunk_info.lock_time = time.time()
		sender.send_obj(chunk_info)
		return True


def srv(settings):
	log("master server start")
	listen_sock_chunk = net.listen_sock(settings.MASTER_CHUNK_PORT)
	listen_sock_client = net.listen_sock(settings.MASTER_CLIENT_PORT)
	chunkserver_req_handler = PakServer(listen_sock_chunk,"chunkhandler")
	client_req_handler = PakServer(listen_sock_client,"clienthandler")
	
	while 1:
		chunkserver_req_handler.tick()
		client_req_handler.tick()
		
		for qk in meta.client_req_queues.keys():
			q = meta.client_req_queues[qk]
			for (req, sock, debug_str) in q:
				log("req queue: " + debug_str)
				if not req.fulfill(sock,meta):
					break				
				# done with req. pop it off the front
				meta.client_req_queues[qk] = meta.client_req_queues[qk][1:]

			
			
			


