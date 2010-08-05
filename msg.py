"""
utility module that contains classes for communication only
"""
import net

import log
import os
import msg

class ChunkConnect:
	"message from a chunkserver when it first connects"

	def __init__(self,sockname,ids):
		'inits this message with all the UIDs for blocks a chunkserver knows'
		self.sockname = sockname
		self.ids = ids
		
	def __call__(self,meta,sock):
		log.log("ChunkConnect, %i ids: %s" % (len(self.ids),str(self.ids)))
		for id in self.ids:
			added = False
			log.log("trying to add " + id)
			for fi in meta.fileinfos.values():
				for ci in fi.chunkinfos:
					if ci.id != id:
						pass
						
					log.log('adding chunkserver %s to file %s' % (str(self.sockname),fi.fname))
					ci.servers.append((self.sockname))
					added = True

			if not added:
				# TODO: gc these chunks?
				log.err("failed to find file for chunk id %s" % id)
				


class ReadReq:
	"message from a clientserver to the master to get a chunk handle"

	def __init__(self,fname,chunk_index,len):
		'inits this message with a read request'
		self.fname = fname
		self.chunk_index = chunk_index
		self.len = len
		
	def __call__(self,meta,sock):
		"""dispatch the call when it reaches the front of the
		queue. returns True if this request should be removed from the
		queue, False otherwise"""
		
		log.log("ReadReq(%s,%i,len=%i)"%(self.fname,self.chunk_index,self.len))
		res = net.PakSender(sock)

		# get the file info
		file_info = meta.fileinfos[self.fname]
		if not file_info:
			res.send_obj(ReadErr("file '%s' not found" % self.fname))
			return 

		# get the chunk info			
		try:
			chunk_info = file_info.chunkinfos[self.chunk_index]
			log.log("ReadReq: sending chunk info " + str(chunk_info))
			res.send_obj(chunk_info)
		except IndexError:
			log.log("ReadReq: chunk index out of bounds %i" % self.chunk_index)
			res.send_obj(ReadErr("chunk '%i' out of range"%self.chunk_index))


class ReadChunk:
	"read a chunk from a chunkserver"
	def __init__(self,id,offset,len):
		self.id = id
		self.offset = offset
		self.len = len

	def __call__(self,chunkserver,sock):
		"get the chunk and send it back"
		fn = os.path.join(chunkserver.chunkdir,self.id + '.chunk')
		log.log('opening chunk ' + fn)
		f = open(fn,'rb')
		sender = net.PakSender(sock)
		
		if not f:
			sender.send_obj(ReadErr("chunk %s not found" % self.id))
			return
		f.seek(self.offset)
		s = f.read(self.len)
		log.log("sending read of chunk " + s)
		sender.send_obj(s)


class ReadErr:
	def __init__(self,str):
		self.errmsg = str

	def __str__(self):
		return "ReadErr: " + self.errmsg

class ChunkHandle:
	""
	def __init__(self,id):
		self.id = id

class FileNotFoundErr(ReadErr):
	def __init__(self,errmsg):
		self.errmsg = errmsg

