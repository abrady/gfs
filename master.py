import sys
import socket
import select 
import cPickle
import net

class ChunkMsg:
	"message from a chunkserver"
	def __call__(self):
		print "ChunkMsg: ", self

def srv(settings):
	
	chunkservers = {}	
	
	def listen_sock(port):
		print "listening on port ", port
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind(('',port))
		s.setblocking(0)
		s.listen(1)
		return s
		
	s_chunk = listen_sock(settings.MASTER_CHUNKPORT)
	s_client = listen_sock(settings.MASTER_CLIENTPORT)

	# select.select

	def handle_clients(s):
		try:
			conn, addr = s.accept()
		except socket.error:
			return # no conn, done
		
		print 'client conn from ', addr
		# TODO client metadata requests
		conn.close()
		return

	def handle_chunkservers(s, cs):
		try:
			conn, addr = s.accept()
		except socket.error:
			return # no conn, done
		# new connection
		print 'chunkserv conn from ', addr

		if(cs.has_key(addr)):
			sys.stderr.write("duplicate address %r, dropping old" % addr)
		cs[addr] = conn

		rds,_,_ = select.select(cs.values(),(),())
		
		# chunks are trusted, just read the objects and execute them
		for r in rds:
			recvr = net.Receiver(r)
			o = recvr.recv_obj()
			print "recv ", o
			o()
			
			#debug
			s_client.close()
			s_chunk.close()
			sys.exit()
			
		
	while 1:
		handle_clients(s_client)
		handle_chunkservers(s_chunk, chunkservers)


