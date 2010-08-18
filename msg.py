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
				master.log('add chunk %s to servers"%s"'%(id,str(added)))

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
		sender = net.PakSender(sock,"master:%s" % (sender_name))
		master.senders.append(sender)

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
		sender = net.PakSender(sock,"%s:%s" % (chunkserver.name,sender_name))
		
		if not f:
			sender.send_obj(Err("chunk %s not found" % self.id))
			return
		f.seek(self.offset)
		s = f.read(self.len)
		chunkserver.log("sending read of chunk " + s)
		sender.send_obj(s)
		chunkserver.senders.append(sender)



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
		sender = net.PakSender(sock,"master:%s" % (sender_name))
		master.senders.append(sender)

		# check for file existance and create if necessary
		if master.meta.fileinfos.has_key(self.fname):
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
	def __init__(self,chunk_info, mutate_id, data):
		self.chunk_info = chunk_info
		self.mutate_id = mutate_id
		self.data = data

	def send_next(self,chunkserver,sock):
		"""coroutine for sending data to the next server, and sending
		success/failure back to the initiator
		"""
		# remove current server from the list.
		try:
			chunk_addr = chunkserver.client_server.sock.gethostname()
			self.chunk_info.servers.remove(chunk_addr)
		except ValueError:
			chunkserver.log("unable to find current chunkserver (%s) in list of chunk owners passed from client. continuing" % str(chunk_addr))

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
		chunkserver.pending_data[mutate_id] = (time.time(),self.mutate_id,self.data)

		# out of servers to forward to, return success
		if not len(self.chunk_info.servers):
			sender = net.PakSender(sock,"SendData:res")
			sender.send_obj(SendDataSuccess(self.mutate_id))
			chunkserver.senders.append(sender)
			return

		# forward to the next chunkserver (and queue it in things to pump)
		chunkserver.data_sends.append(self.send_next(chunkserver.sock))

		

		
			


class CommitAppendReq:
	"""message from client to master asking to commit data
	"""
	def __init__(self,chunk_info):
		self.chunk_info = chunk_info

	def commit(self,master,client_sock):
		for addrinfo in self.chunk_info.servers:
			chunk_sock = master.chunksrv_server.client_socks[addrinfo]
			if not chunk_sock:
				net.PakSender(client_sock).send_obj(msg.Err("couldn't get sock for chunkserver addr (%s);" % str(addrinfo)))
				return

			chunk = net.PakComm(chunk_sock)
			chunk.send_obj(WriteData(chunk_info))
				

	def __call__(self,master,sock):
		# TODO get each chunkserver to commit the data
		# update the chunk info locally
		# need a way to do tis async...
		# make a PakComm queue?
		pass

class WriteData:
	"""Message from master to chunkservers to commit data
	"""
	def __init__(self,chunk_info,mutate_id, data):
		self.chunk_info = chunk_info
		self.mutate_id = mutate_id
		self.data = data

	def __call__(self,chunkserver,sock):
		data = chunkserv.pending_data[self.mutate_id]
		if not data:
			PakSender(sock).send_obj(Err("couldn't open file %s"%fname))
			return
		
		fname = os.path.join(chunkserver.chunkdir,chunk_info.id + ".chunk")
		f = open(fname,"w")
		if not f:
			PakSender(sock).send_obj(Err("couldn't open file %s"%fname))
			return

		f.seek(self.chunk_info.len)
		#f.write(


