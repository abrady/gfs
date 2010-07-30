import sys
import socket
import cPickle
import select
from log import log

PAK_VER = 20100720

class VersionMismatch(Exception):
	def __init__(self,value):
		self.value = value
	def __str__(self):
		return self.value

class PakSender:
	"dumb packet switched class"
	def __init__(self,sock):
		self.sock = sock

	def _send_int(self, n):
		n = "%16i" % n
		print "sending ", n
		self.sock.send(n)

	def send_obj(self, obj):
		# send header
		self._send_int(PAK_VER)
		p = cPickle.dumps(obj)
		self._send_int(len(p))
		print "sending obj", p
		self.sock.send(p)

class PakReceiver:
	def __init__(self,sock):
		self.sock = sock

	def _recv_int(self):
		s = self.sock.recv(16)
		if not s:
			return 0
		return int(s)

	def recv_obj(self):
		ver = self._recv_int()
		if not ver:
			return None 
		if(ver != PAK_VER):
			raise VersionMismatch("recv_obj")
		n = self._recv_int()
		s = self.sock.recv(n)
		return cPickle.loads(s)

class PakComm(PakReceiver,PakSender):
	"helper for combining a sender and receiver"
	def __init__(self,sock):
		self.sock = sock

class PakClientMsg:
	'dummy class that shows a callable client message'
	def __init__(self,str):
		self.str = str
		
	def __call__(self):
		log("PakClientMsg: %s" % self.str)

class PakServer:
	"""handles a listen socket that receives packets from a PakSender
	- listens for and accepts new connections
	- keeps a list of clients that have connected
	- removes clients that have disconnected
	- receives objects from clients via the PakSend/Recv interface
	- invokes sent objects via the callable interface"""

	def __init__(self, listen_sock, name):
		self.listen_sock = listen_sock
		self.client_socks = []
		self.name = name

	def tick(self):
		log("checking for accepts")
		try:
			conn, addr = self.listen_sock.accept()
			log('%s conn from %s' % (self.name, str(addr)))
			self.client_socks.append(conn)
		except socket.error:
			pass # no conn

		if not self.client_socks:
			return
		
		log("checking for reads")
		rds,_,_ = select.select(self.client_socks,[],[],0)

		log("reading %i sockets" % len(rds))
		for r in rds:
			recvr = PakReceiver(r)
			if recvr:
				obj = recvr.recv_obj()
				log("recv " + str(obj))
				obj()
			else:
				# client disconnect, drop it
				r.close()
				self.client_socks.remove(r)			

def listen_sock(port):
	log("listening on port %i" % port)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(('',port))
	s.setblocking(0)
	s.listen(1)
	return s

def client_sock(addr,port):
	log('connecting to ' + addr + ' ' + str(port))
	c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c.setblocking(0)
	try:
		c.connect((addr,port))
	except socket.error:
		pass
	return c

def test():
	port = 12345
	ps = PakServer(listen_sock(port),"testserver")
	pc0 = PakSender(client_sock('localhost',port))
	pc1 = PakSender(client_sock('localhost',port))
	ps.tick() # should see 2 accepts
	pc0.send_obj(PakClientMsg("0: hello port %i" % port))
	pc1.send_obj(PakClientMsg("1: hello port %i" % port))
	ps.tick()
	del ps
