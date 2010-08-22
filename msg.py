"""
utility module that contains classes for communication only
"""
import log
import os
import msg
import time

import net


# *************************************************************************
# helper routines 
# *************************************************************************


def handle_req_response(comm,req,log):
	"""Helper coroutine for a PakComm object and the async send-recv process:
	- send object
	- wait for response
	- check for Err response type

	The typical way to use this is in your coroutine to go:
		comm = net.PakComm((addr,port),'')
		for res in msg._handle_req_response(chunk_comm,msg_obj,log):
			if res:
				break;
			yield None
		if not res:
			return
	"""
	log("sending %s on %s" % (req,comm))
	comm.send_obj(req)
	while not comm.can_recv():
		log("can't receive")
		comm.tick()
		yield None
	res = comm.recv_obj()	
	if not res:
		log("connection closed (failed to recv object)")
		return
	if isinstance(res,Err):
		log("error from comm %s" % str(res))
		return
	yield res


# *************************************************************************
# Error messages  
# *************************************************************************


class Err:
	"""class for errors in communication.
	"""
	def __init__(self,str):
		self.errmsg = str
	def __str__(self):
		return "Err: " + self.errmsg


class FileNotFoundErr(Err):
	def __init__(self,errmsg):
		self.errmsg = errmsg


# *************************************************************************
# master - chunk messages
# *************************************************************************


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
			master.log("adding info for id %s" % id)
			for fi in meta.fileinfos.values():
				#master.log("checking file %s" % fi.fname)
				for ci in fi.chunkinfos:
					if ci.id != id:
						continue
					master.log("adding chunkserver %s to file %s(%s)" % (str(self.csid),fi.fname,ci.id))
					ci.servers.append(self.csid)
					added.append(fi.fname)

			if not len(added):
				# TODO: save this info for cleanup
				log.err("failed to find file for chunk id %s" % id)
			else:
				# okay for same chunk to be in multiple files
				#master.log('add chunk %s to file "%s"'%(id,str(added)))
				pass

# *************************************************************************
# Read  
# *************************************************************************

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
		sender = master.make_tracked_sender(sock,sender_name)

		# get the file info
		file_info = meta.fileinfos[self.fname]
		if not file_info:
			sender.send_obj(Err("file '%s' not found" % self.fname))
			return 

		# get the chunk info			
		try:
			chunk_info = file_info.chunkinfos[self.chunk_index]
			master.log("ReadReq: sending chunk info " + str(chunk_info))
			sender.send_obj(chunk_info)
		except IndexError:
			master.log("ReadReq: chunk index out of bounds %i" % self.chunk_index)
			sender.send_obj(Err("chunk '%i' out of range"%self.chunk_index))


class ReadChunk:
	"message from client to chunkserver: read a chunk from a chunkserver"
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
		sender = chunkserver.make_tracked_sender(sock,sender_name)
		
		if not f:
			sender.send_obj(Err("chunk %s not found" % self.id))
			return
		f.seek(self.offset)
		s = f.read(self.len)
		chunkserver.log("sending read of chunk " + s)
		sender.send_obj(s)



# *************************************************************************
#  Append 
# *************************************************************************


class AppendReq:
	'''message from a client to the master to get a chunk handle for writing.
	case 0: enough room to add the data
	- reserve space in that chunk for the append (transient?)
	- (usual write thing)
	case 1: 
	  ...
	'''
	def __init__(self,fname,len):
		"init this append operation with the filename, and how much data is intended for write"
		self.fname = fname
		self.len = len

	def __call__(self,master,sock):
		"""dispatch the call when it reaches the front of the
		queue. returns True if this request should be removed from the
		queue, False otherwise"""
		master.log("AppendReq(%s)" % self.fname)
		sender_name = str(sock.getsockname())
		sender = master.make_tracked_sender(sock,sender_name)

		# check for file existance and create if necessary
		if self.fname in master.meta.fileinfos:
			file_info = master.meta.fileinfos[self.fname]
		else:
			file_info = master._create_file(self.fname)
			if not file_info:
				sender.send_obj(Err("file '%s' not found" % self.fname))
				return		

		# get the last chunk and see if there is room to add more data
		chunk_info = master.req_append(file_info,self.len)
		if(chunk_info):
			master.log("sending info about chunk id %s" % chunk_info.id)
			sender.send_obj(chunk_info)
			return
		log("TODO: append failed, need to alloc new block")
		# if we get here we have to allocate a new chunk
		# TODO
		# chunk_info = master.add_chunk_to_file(fname)
		# if not chunk_info:
		# 	sender.send_obj(Err("couldn't alloc chunkid for %s" % self.fname))
		# 	return
		# sender.send_obj((chunk_info)


class SendDataSuccess:
	def __init__(self,mutate_id):
		self.mutate_id = mutate_id
		

class SendData:
	"""Message from client to chunkserver to put data in an LRU buffer
	for commit at a later date (identified by UID mutate_id from master)
	"""
	def __init__(self,chunk_info, data):
		self.chunk_info = chunk_info
		self.data = data

	def send_next(self,chunkserver,sock):
		"""coroutine for sending data to the next server, and sending
		success/failure back to the initiator
		"""
		# 'recurse' into this call for the next chunkserver that owns this data
		comm = net.PakComm(self.chunk_info.servers[0],"SendData:rec")
		comm.send_obj(self)
		while not comm.can_recv():
			chunkserver.log("can't receive")
			comm.tick()
			yield None
		res = comm.recv_obj()

		sender = net.PakSender(sock,"SendData:res-fwd")
		sender.send_obj(res)
		while sender.send_pending():
			yield None


	def __call__(self,chunkserver,sock):
		# Queue the data, that is all
		mutate_id = self.chunk_info.mutate_id
		chunkserver.pending_data[mutate_id] = (time.time(),mutate_id,self.data)

		# remove current server from the list.
		try:
			chunk_addr = chunkserver.name()
			self.chunk_info.servers.remove(chunk_addr)
		except ValueError:
			chunkserver.log("unable to find current chunkserver (%s) in list of chunk owners passed from client. continuing" % str(chunk_addr))

		# out of servers to forward to, return success
		if not len(self.chunk_info.servers):
			chunkserver.log("done forwarding data. responding to sock %s" % str(sock.getpeername()))
			sender = chunkserver.make_tracked_sender(sock,"SendData:res")
			sender.send_obj(SendDataSuccess(mutate_id))
			return

		# forward to the next chunkserver (and queue it in things to pump)
		chunkserver.log("forwarding data to next server %s" % (str(self.chunk_info.servers)))
		chunkserver.data_sends.append(self.send_next(chunkserver,sock))


class CommitAppendReq:
	"""message from client to master asking to commit data
	"""
	def __init__(self,chunk_info):
		self.chunk_info = chunk_info

	def commit(self,master,client_sock):
		master.log("comitting to chunkservers %s" % str(self.chunk_info.servers))
		pending = []

		for addrinfo in self.chunk_info.servers:
			master.log("chunkserver %s" % str(addrinfo))

			# TODO: super dumb serial commits
			# also pretty dumb: don't use the existing master sockets but I'm lazy
			chunk = net.PakComm(addrinfo,"master:%s" % str(addrinfo))
			for res in handle_req_response(chunk,WriteData(self.chunk_info),master.log):
				if res:
					break
				yield None
			if not res:
				s = "commit failed for chunkserver %s. failing" % str(addrinfo)
				master.log(s)
				master.send_obj_to(addrinfo,Err(s))
				return
		master.send_obj_to(client_sock,CommitSuccess(self.chunk_info.mutate_id), "commit %i" % self.chunk_info.mutate_id)
		master.log("successful commit for mutation %i" % res.mutate_id)

	def __call__(self,master,sock):
		master.pending_commits.append(self.commit(master,sock))


class CommitSuccess:
	"tell requestor the commit was successful"
	def __init__(self, mutate_id):
		self.mutate_id = mutate_id

		
class WriteDataSuccess:
	def __init__(self,mutate_id):
		self.mutate_id = mutate_id

	def __call__(self,master,sock):
		master.log("TODO: successfull commit")

class WriteData:
	"""Message from master to chunkservers to commit data
	"""
	def __init__(self,chunk_info):
		self.chunk_info = chunk_info

	def __call__(self,chunkserver,sock):
		time,mutate_id,data = chunkserver.pending_data[self.chunk_info.mutate_id]
		sender = chunkserver.make_tracked_sender(sock)
		if not data:
			sender.send_obj(Err("couldn't open file %s"%fname))
			return
		
		fname = os.path.join(chunkserver.chunkdir,self.chunk_info.id + ".chunk")
		f = open(fname,"w")
		if not f:
			sender.send_obj(Err("couldn't open file %s"%fname))
			return

		f.seek(self.chunk_info.len)
		f.write(data)
		sender.send_obj(WriteDataSuccess(self.chunk_info.mutate_id))
