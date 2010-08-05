#TODO logging changes to fs

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
import time
import settings
import os
import time

# package modules
import msg
import net
import log

try:
	import settings # Assumed to be in the same directory.
except ImportError:
	sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. This is required\n" % __file__)
	sys.exit(1)
	
	if(settings.DEBUG):
		reload(settings)

class ChunkInfo:
	"contains the id of a chunk, and the servers that manage it"
	def __init__(self,chunk_id,servers):
		self.id = chunk_id
		self.servers = servers


class FileInfo:
	"contains list of chunks by offset and any other file info"
	def __init__(self,fname):
		self.fname = fname
		self.chunkinfos = []


class Meta:
	""" the info about the filesystem itself. in the real GFS this data
	is critical, replicated, logged, etc.

	fileinfos -- hash lookup of a filename to info
	"""
	def __init__(self):
		self.fileinfos = {}
		

class MasterServer:
	"""Server class for the 'master' of the gfs
	"""

	def __init__(self):
		self.log("master server start")
		s = net.listen_sock(settings.MASTER_CHUNK_PORT)
		self.chunksrv_server = net.PakServer(s,"chunkserverhandler")

		s = net.listen_sock(settings.MASTER_CLIENT_PORT)
		self.client_server = net.PakServer(s,"clienthandler")
		
		# meta data
		fn = settings.MASTER_META_FNAME
		if(os.path.exists(fn)):
			self.meta = cPickle.load(open(fn,'rb'))
			self.log('meta(%s) loaded: ' % fn + str(self.meta))
		else:
			self.meta = Meta()
			self.log('making new meta %s: ' % fn + str(self.meta))
			
				
	def tick(self):
		def req_handler(req,sock):
			"callback for servers. dispatches the object with meta"
			self.log(' %s on sock %s' % (str(req), sock))
			req(self.meta,sock)

		self.chunksrv_server.tick(req_handler)
		self.client_server.tick(req_handler)

	def log(self, str):
		log.log("[master] " + str)

			
def write_test_meta():
	meta = Meta()
	fi = FileInfo('foo')
	fi.chunkinfos.insert(0,ChunkInfo('0',[]))
	meta.fileinfos['foo'] = fi
	f = open(settings.MASTER_META_FNAME,'wb')
	cPickle.dump(meta,f)
	f.close()

if __name__ == "__main__":
	master = MasterServer()
	frame_rate = 1/30
	while True:
		t = time.time()
		master.tick()
		dt = time.time() - t
		if dt < frame_rate:
			time.sleep(frame_rate - dt)
