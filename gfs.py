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

	log.log("master-chunkserver handshake")
	master = master.MasterServer()      # start + load meta
	chunk = chunkserver.ChunkServer()  # init + load chunks
	chunk.write_test_chunk()
	master.tick()  # connection from chunkserver
	chunk.tick()    # send ChunkConnect
	master.tick()  # recv ChunkConnect

	log.log("dropping chunkserver")
	master.drop_chunkserver(master.chunkservers.keys()[0])
	chunk.tick() # lost conn to master, reconnecting
	master.tick()
	chunk.tick() # send ChunkConn msg
	master.tick() # recv ChunkConn msg, add chunkserver

	log.log("testing FileInfo fetch")
	global fir, fi
	fir = client.file_info('foo')
	fir.next()
	master.tick()
	fi = fir.next()
	log.log("fetched. length %i" % fi.length())
	
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
	expected = "abcdefghijklmnopqrstuvwxyzABCDEF"
	if s != expected:
		log.err("got %s expected %s" % (s,expected))
	
	log.log("appending data")
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
	res = a.next()          # receive success
	log.log("wrote mutate_id(%i)" % res.mutate_id)

	log.log("second FileInfo fetch")
	global fi2r, fi2
	fi2r = client.file_info('foo')
	fi2r.next()
	master.tick()
	fi2 = fi2r.next()
	log.log("fetched. length %i" % fi2.length())
	expected = fi.length() + len(data)
	if expected != fi2.length():
		log.err("file length mismatch. expected %i, received %i" % (expected,fi2.length()))

	log.log("read what was written")
	global r
	r = client.read("foo",fi.length(),len(data))
	for data2 in r:
		log.log("ticking...")
		if data2:
			break
		master.tick()
		chunk.tick()

	log.log("read2 %s" % data2)
	if data2 != data:
		log.err("data mismatch: appended %s, read %s" % (data, data2))
	
	
	
# todo: try the failure states for each step of the read
# todo: chunk_info still has server info for two servers as of the append test.






