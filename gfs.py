import socket
import chunkserver
import master
import client
import net
import random
import log
import msg

try:
	import settings # Assumed to be in the same directory.
except ImportError:
	sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. This is required\n" % __file__)
	sys.exit(1)
	
	if(settings.DEBUG):
		reload(settings)


# testing, remove
# execute()

if(settings.DEBUG):
	reload(settings)
	reload(chunkserver)
	reload(master)
	reload(client)
	reload(net)
	reload(msg)

if(settings.TESTING):
	import thread
	
	def random_port():
		return int(random.random()*32000 + 32000)

	# set up debug data
	settings.MASTER_META_FNAME = 'meta_test.obj'
	settings.MASTER_CHUNK_PORT = random_port()
	settings.MASTER_CLIENT_PORT = settings.MASTER_CHUNK_PORT + 1
	settings.CHUNK_CLIENT_PORT = settings.MASTER_CLIENT_PORT + 1
	log.log("[gfs testing] chunk port %i, master client port %i chunk client port %i" % (settings.MASTER_CHUNK_PORT, settings.MASTER_CLIENT_PORT,settings.CHUNK_CLIENT_PORT))
	master.write_test_meta()
	global master
	global chunk
	master = master.MasterServer()      # start + load meta
	chunk = chunkserver.ChunkServer()  # init + load chunks
	chunk.write_test_chunk()
	master.tick()  # connection from chunkserver
	chunk.tick()    # send ChunkConnect
	master.tick()  # recv ChunkConnect
	# master - chunk handshake done
	log.log("dropping chunkserver")
	master.drop_chunkserver(master.chunkservers.keys()[0])
	chunk.tick() # lost conn to master, reconnecting
	master.tick()
	chunk.tick() # send ChunkConn msg
	master.tick() # recv ChunkConn msg, add chunkserver

	log.log("client read")
	global r
	r = client.read("foo",0,32)
	r.next() # connects to master, sends ReadReq
	master.tick() # get conn
	master.tick() # get ReadReq
	r.next()  # send ReadChunk to chunkserver
	chunk.tick()  # get ReadChunk, send response
	s = r.next()
	log.log("received: %s" %s)
	
	# #	r = client.read("foo",0,32)
	# #	r.next() # connect to master
	# #	master.tick()
	# #	master.client_server.

	data = "1234567890"
	global a
	a = client.append("foo",data)
	a.next()           # connect to master, send request
	master.tick()    # get AppendReq,  
	a.next()           # connecting to chunkserver, SendData
	chunk.tick()     # recv SendData
	a.next()           # get success for mutate 1, send commit to master
	master.tick()   # send commit msg to chunk
	chunk.tick()     # write data
	master.tick()   # get response, done with commit,
	master.tick()   # send client success
	a.next()          # receive success
	
# todo: try the failure states for each step of the read
# todo: chunk_info still has server info for two servers as of the append test.
