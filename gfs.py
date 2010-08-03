import socket
import chunkserver
import master
import client
import net
import random
import log

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

if(settings.TESTING):
	import thread
	
	def random_port():
		return int(random.random()*32000 + 32000)

	# set up debug data
	settings.MASTER_META_FNAME = 'meta_test.obj'
	settings.MASTER_CHUNK_PORT = random_port()
	settings.MASTER_CLIENT_PORT = settings.MASTER_CHUNK_PORT + 1
	settings.CHUNK_CLIENT_PORT = settings.MASTER_CLIENT_PORT + 1
	log.log("chunk port %i, master client port %i chunk client port %i" % (settings.MASTER_CHUNK_PORT, settings.MASTER_CLIENT_PORT,settings.CHUNK_CLIENT_PORT))
	master.write_test_meta()
	global master
	global chunk
	master = master.MasterServer()
	chunk = chunkserver.ChunkServer()
