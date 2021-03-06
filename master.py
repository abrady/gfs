#TODO logging changes to fs rather than synchronous write
#TODO delete orphaned chunks on chunkserver
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
	'''contains the id of a chunk, and the servers that manage it.
	Has the following properties:
	- id : the globally unique chunk id
	- servers : list of chunkservers that claim ownership of this chunk
	- len : the number of bytes written to the chunk so far
	'''
	def __init__(self,chunk_id,servers=[]):
		self.id = chunk_id
		self.servers = servers
		self.len = 0
		self.len_pending = 0 # length from outstanding writes

	def length(self):
		return self.len + self.len_pending


class FileInfo:
	"contains list of chunks by offset and any other file info"
	def __init__(self,fname):
		self.fname = fname
		self.chunkinfos = []

	def length(self):
		"length of this file"
		n = 0
		for c in self.chunkinfos:
			n += c.length()
		return n


class Meta:
	""" the info about the filesystem itself. in the real GFS this data
	is critical, replicated, logged, etc.

	fileinfos -- hash lookup of a filename to info
	"""
	def __init__(self):
		self.fileinfos = {}
		self.max_chunk_id = 0

class MasterServer(net.PakSenderTracker):
	"""Server class for the 'master' of the gfs
	"""
	def __init__(self):
		self.log("master server start")
		s = net.listen_sock(settings.MASTER_CHUNK_PORT)
		self.chunksrv_server = net.PakServer(s,"master:chunksrv")

		s = net.listen_sock(settings.MASTER_CLIENT_PORT)
		self.client_server = net.PakServer(s,"master:clientsrv")
		
		# meta data
		fn = settings.MASTER_META_FNAME
		if(os.path.exists(fn)):
			self.meta = cPickle.load(open(fn,'rb'))
			self.log('meta(%s) loaded: ' % fn + str(self.meta))
		else:
			self.meta = Meta()
			self.log('making new meta %s: ' % fn + str(self.meta))

		# pak senders: things with network responses that might not
		# be able to send right away
		self.senders = []

		# connected chunkservers
		self.chunkservers = {}

		# guid of alterations
		self.max_mutate_id = 0

		# coroutines of in progress commits of appends, writes, etc. 
		self.pending_commits = []
				
	def tick(self):
		def req_handler(req,sock):
			"callback for servers. dispatches the object with meta"
			self.log(' %s on sock %s' % (str(req), sock))
			req(self,sock)

		self.chunksrv_server.tick(req_handler)
		self.client_server.tick(req_handler)

		self.tick_senders()

		# pump any coroutines
		for ds in self.pending_commits[:]:
			try:
				self.log("pumping pending_commit %s" % str(ds))
				ds.next()
			except StopIteration:
				self.log("done with %s" % str(ds))
				self.pending_commits.remove(ds)
		
	def drop_chunkserver(self,csid):
		'''remove this chunkserver from all chunks that reference it
		'''
		if csid in self.chunkservers:
			cs = self.chunkservers[csid]
			self.log("dropping chunkserver %s, socket %s" % (str(csid), str(cs)))
			self.chunksrv_server.close_client(cs)
		
		# todo, something less stupid
		# remove the chunkserver from the list of servers associated
		# with each chunk
		for fi in self.meta.fileinfos:
			removed = []
			fname = self.meta.fileinfos[fi].fname
			for ci in self.meta.fileinfos[fi].chunkinfos:
				try:
					#self.log("trying to remove %s from %s (%s)" % (str(csid),ci.id,ci.servers))
					ci.servers.remove(csid)
					removed.append(ci.id)
					#self.log("removed %s" % str(ci.servers))
					#self.log("aaand....%s" % self.meta.fileinfos['foo'].chunkinfos[0].servers)
				except ValueError:
					#self.log("couldn't remove %s from chunk %s" % (str(csid), ci.id))
					pass
			if len(removed):
				self.log("\tfrom %s removed chunks %s" % (fname, str(removed)))
			else:
				self.log("\tremove chunkserver %s from nothing" % (str(csid)))

	def _alloc_chunkid(self):
		"allocate a new globally unique chunkid"
		# TODO: journal this
		self.meta.max_chunk_id += 1
		return str(self.meta.max_chunk_id)

	def _create_file(self,fname):
		"update meta info with a new file"
		self.log("create file %s" % fname)
		if self.meta.fileinfos[fname]:
			self.log("file %s already exists" % fnme)
			return
		fi = FileInfo(fname)
		self.meta.fileinfos[fname] = fi

		# update meta synchronously.
		# TODO: background, log, replicate, etc.
		self.log("serializing meta to disk")
		f = open(settings.MASTER_META_FNAME,'wb')
		cPickle.dump(meta,f)
		f.close()
		self.log("done writing meta")
	
	def add_chunk_to_file(self,fname):
		'''allocate a new chunk for the passed file name.
		returns None on failure
		returns a ChunkInfo object initialized with a new id, and a set of servers to own that chunk.
		'''
		# TODO: journal this
		file_info = self.meta.fileinfos[fname]
		if not file_info:
			return None
		cid = self._alloc_chunkid()
		self.log("adding chunk %s to file %s" % (cid,fname))
		file_info.chunkinfos.append(ChunkInfo(cid))
		return cid

	def req_append(self,file_info,append_len):
		"reserve space for appending an amount of data"
		self.log("req append(%s,%i bytes)" % (file_info.fname,append_len))

		n = len(file_info.chunkinfos)
		if not n:
			self.log("TODO no chunkinfos to append to")
			return None
		
		chunk_info = file_info.chunkinfos[n-1]
		if chunk_info.length() + append_len > settings.CHUNK_SIZE:
			self.log("can't append to chunk %s, full" % chunk_info.id)
			return None 

		chunk_info.len_pending += append_len
		self.max_mutate_id += 1
		chunk_info.mutate_id = self.max_mutate_id
		#self.log("mutating chunk %s" % chunk_info.id)
		return chunk_info
		
	def log(self, str):
		log.log("[master] " + str)

			
def write_test_meta():
	meta = Meta()
	fi = FileInfo('foo')
	ci = ChunkInfo('1',[])
	ci.len = 64
	fi.chunkinfos.insert(0,ci)
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
