"""
utility module that contains classes for communication only
"""

class ChunkConnect:
	"message from a chunkserver when it first connects"

	def __init__(self,ids):
		'inits this message with all the UIDs for blocks a chunkserver knows'
		self.ids = ids
		
	def __call__(self,meta):
		log("ChunkConnect")


class ClientRead:
	"message from a clientserver when it first connects"

	def __init__(self,fname,chunk_index,len):
		'inits this message with a read request'
		self.fname = fname
		self.chunk_index = chunk_index
		self.len = len
		
	def fulfull(self,meta,sock):
		"""dispatch the call when it reaches the front of the
		queue. returns True if this request should be removed from the
		queue, False otherwise"""
		
		log("ClientRead(%s,%i,len=%i"%(self.fname,self.offset,self.len))
		res = PakSender(sock)

		# get the file info
		file_info = meta.fileinfos[self.fname]
		if not file_info:
			res.send_obj(ReadErr("file '%s' not found" % self.fname))
			return 

		# get the chunk info
		chunk_info = file_info.chunks[chunk_index]
		if not chunk_info:
			res.send_obj(ReadErr("chunk '%i' not found"%chunk_index))
			return
		res.send_obj(chunk_info)
		return True

class ReadErr:
	def __init__(self):
		self.errmsg = "ReadErr"
	

class FileNotFoundErr(ReadErr):
	def __init__(self,errmsg):
		self.errmsg = errmsg
