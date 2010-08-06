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
	global client
	master = master.MasterServer()      # start + load meta
	chunk = chunkserver.ChunkServer()  # init + load chunks
	chunk.write_test_chunk()
	master.tick()  # connection from chunkserver
	chunk.tick()    # send ChunkConnect
	master.tick()  # recv ChunkConnect
	# master - chunk handshake done
	master.chunksrv_server.client_socks[0].close()
	master.chunksrv_server.client_socks = []
	chunk.tick() # lost conn to master
	master.tick()
	chunk.tick() # send ChunkConn msg
	master.tick() # recv ChunkConn msg

	# todo: proper chunk timeout, proper cleanup of chunk loss
	
#	client = client.read("foo",0,32)
#	client.next()
#	master.tick() # should get read request
#	client.next()
#	chunk.tick()
#	client.next()
