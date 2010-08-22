"""
In the GFS the client's job is to:
- request reads and mutations from the master
- write data to all chunkservers
- retry failures
- break writes into small enough chunks to succeed
- manage any locking or operation ordering (in GFS two clients can
  write to the same chunk in an append, both writes may succeed but
  the order will be uncontrolled by the GFS)

Client operations:
- create
- delete
- open
- close
- read
- write
- snapshot
- append
"""
import random
import cPickle

# package modules
import net
import msg
import master
from log import log as _log # cheesy

try:
	import settings # Assumed to be in the same directory.
except ImportError:
	sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. This is required\n" % __file__)
	sys.exit(1)
	
	if(settings.DEBUG):
		reload(settings)

def log(str):
	_log("[client] " + str)

def read(fname, offset, len):
	"""involves these steps:
	1 request (fname/offset) from the master
	1a master puts read lock on fname
      2 receive a chunk handle and set of chunkservers with chunk
	3 pick a chunkserver (nearest), request the data
	4 close handle on the master
	4a master releases lock
	"""
	def log(str):
		_log("[client:read] " + str)
		
	log("read(%s,%i,%i), connecting to (%s,%i)"%(fname,offset,len,settings.MASTER_ADDR, settings.MASTER_CLIENT_PORT))
	log("connecting to master")
	master_comm  = net.PakComm((settings.MASTER_ADDR, settings.MASTER_CLIENT_PORT),"client:read")
	chunk_index = offset/settings.CHUNK_SIZE

	req = msg.ReadReq(fname,chunk_index,len)
	for chunk_info in msg.handle_req_response(master_comm,req,log):
		if chunk_info:
			break
		yield None		
	if not chunk_info:
		return

	# pick a chunkserver to talk to
	#log("chunk_info: %s" % cPickle.dumps(chunk_info))
	random.shuffle(chunk_info.servers)
	chunkaddr,port = chunk_info.servers[0]
	log("picked server (%s,%i)" % (chunkaddr,port))
	chunk_comm = net.PakComm((chunkaddr,port),"client:read")

	log("sending read req")
	read_req = msg.ReadChunk(chunk_info.id,offset,len)
	for read_res in msg.handle_req_response(chunk_comm,read_req,log):
		if read_res:
			break
		yield None
	if not read_res:
		log("lost connectiont to chunkserver")
		return
	yield read_res


def append(fname, data):
	"append a block of data to a file"
	def log(str):
		_log("[client:append] " + str)

	log("append(%s,len=%i )" %(fname,len(data)))
	log("talking to master")
	append_req = msg.AppendReq(fname,len(data))
	master_comm  = net.PakComm((settings.MASTER_ADDR, settings.MASTER_CLIENT_PORT),"client:append")
	for res in msg.handle_req_response(master_comm,append_req,log):
		if res:
			break
		yield None
	if not res:
		log("no response from master on append request. failing")
		return
	chunk_info = res

	# TODO another deviation: in real GFS, 'closest' chunkserver would be
	# picked using some heuristic
	random.shuffle(chunk_info.servers)
	chunkaddr,port = chunk_info.servers[0]
	log("mutate id %i from master. connecting to chunkserver (%s,%i) (picked from %s)" % (chunk_info.mutate_id,chunkaddr,port,str(chunk_info.servers)))
	chunk_comm = net.PakComm((chunkaddr,port),"client:append")
	write_req = msg.SendData(chunk_info,data)
	for res in msg.handle_req_response(chunk_comm,write_req,log):
		if res:
			break;
		yield None
	if not res:
		log("no response from chunkservers on Senddata. failing")
		return
	log("append %i succeeded got %s from chunkserver" % (res.mutate_id,str(res)))
	commit_req = msg.CommitAppendReq(chunk_info)
	for res in msg.handle_req_response(master_comm,commit_req,log):
		if res:
			break
		yield None
	if not res:
		log("no respone from master on commit. failing")
		return
	
	# TODO any kind of retrying
	log("%i write complete " % res.mutate_id)
	yield res
