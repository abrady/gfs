"""
utility module that contains classes for communication only
"""
import net

import log
import os
import msg

class ReadReq:
	"message from a clientserver to the master to get a chunk handle"

	def __init__(self,fname,chunk_index,len):
		'inits this message with a read request'
		self.fname = fname
		self.chunk_index = chunk_index
		self.len = len
		
	def __call__(self,master,sock):
		"""dispatch the call when it reaches the front of the
		queue. returns True if this request should be removed from the
		queue, False otherwise"""
		meta = master.meta
		master.log("ReadReq(%s,%i,len=%i)"%(self.fname,self.chunk_index,self.len))
		sender_name = str(sock.getsockname())
		sender = net.PakSender(sock,"master:%s" % (sender_name))
		master.senders.append(sender)

		# get the file info
		file_info = meta.fileinfos[self.fname]
		if not file_info:
			sender.send_obj(ReadErr("file '%s' not found" % self.fname))
			return 

		# get the chunk info			
		try:
			chunk_info = file_info.chunkinfos[self.chunk_index]
			master.log("ReadReq: sending chunk info " + str(chunk_info))
			sender.send_obj(chunk_info)
		except IndexError:
			master.log("ReadReq: chunk index out of bounds %i" % self.chunk_index)
			sender.send_obj(ReadErr("chunk '%i' out of range"%self.chunk_index))


class ChunkConnect:
	"message from a chunkserver when it first connects"

	def __init__(self,csid,ids):
		'inits this message with all the UIDs for blocks a chunkserver knows'
		self.csid = csid
		self.ids = ids
		
	def __call__(self,master,sock):
		meta = master.meta
		master.log("ChunkConnect,%s,%i ids:%s" % (str(self.csid),len(self.ids),str(self.ids)))

		master.drop_chunkserver(self.csid)
		master.chunkservers[self.csid] = (sock) 
		
		for id in self.ids:
			added = [] 
			for fi in meta.fileinfos.values():
				for ci in fi.chunkinfos:
					if ci.id != id:
						pass
					ci.servers.append(self.csid)
					added.append(fi.fname)

			if not len(added):
				log.err("failed to find file for chunk id %s" % id)
			else:
				# okay for same chunk to be in multiple files
				master.log('adding chunk %s to files "%s"' % (id,str(added)))


class ReadChunk:
	"read a chunk from a chunkserver"
	def __init__(self,id,offset,len):
		self.id = id
		self.offset = offset
		self.len = len

	def __call__(self,chunkserver,sock):
		"get the chunk and send it back"
		fn = os.path.join(chunkserver.chunkdir,self.id + '.chunk')
		chunkserver.log('opening chunk ' + fn)
		f = open(fn,'rb')
		sender_name = str(sock.getsockname())
		sender = net.PakSender(sock,"%s:%s" % (chunkserver.name,sender_name))
		
		if not f:
			sender.send_obj(ReadErr("chunk %s not found" % self.id))
			return
		f.seek(self.offset)
		s = f.read(self.len)
		chunkserver.log("sending read of chunk " + s)
		sender.send_obj(s)
		chunkserver.senders.append(sender)


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

