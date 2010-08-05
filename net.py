import sys
import socket
import cPickle
import select
from log import log as _log
import msg

PAK_VER = 20100720

logging_enabled = True # False

def log(str):
	if(logging_enabled):
		_log(str)

def can_recv(sock):
	rds,_,_ = select.select([sock],[],[],0)
	return len(rds) > 0

def can_send(sock):
	_,ws,_ = select.select([],[sock],[],0)
	return len(ws) > 0

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
		log("send_int %i" % n)
		n = "%16i" % n
		self.sock.send(n)

	def send_obj(self, o):
		# send header
		self._send_int(PAK_VER)
		log("send_obj " + str(o))
		p = cPickle.dumps(o) # if it fails here, obj probably not pickle-able
		self._send_int(len(p))
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
		if not can_recv(self.sock):
			return None
		
		ver = self._recv_int()
		if not ver:
			log("recv_obj: socket closed on other end")
			return None 
		if(ver != PAK_VER):
			raise VersionMismatch("recv_obj")
		n = self._recv_int()
		s = self.sock.recv(n)
		log("loading " + s)
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

	def tick(self, obj_handler=None):
		"""
		Check for any incoming connections, and listen for any objects
		from open connections.

		obj_handler -- optional callback function, if this is None, any received objects are invoked via the __call__ interface
		"""
		try:
			conn, addr = self.listen_sock.accept()
			log('%s conn from %s' % (self.name, str(addr)))
			self.client_socks.append(conn)
		except socket.error:
			pass # no conn

		if not self.client_socks:
			return
		
		rds,_,_ = select.select(self.client_socks,[],[],0)

		if(len(rds)):
			log("reading %i sockets" % len(rds))
			
		for r in rds:
			recvr = PakReceiver(r)
			obj = recvr.recv_obj()
			if obj:
				log("recv " + str(obj))
				if(obj_handler):
					obj_handler(obj,r)
				else:
					obj(r)
			else:
				# client disconnect, drop it
				self.client_socks.remove(r)			
				r.close()

def listen_sock(port):
	log("listening on port %i" % port)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(('',port))
	s.setblocking(0)
	s.listen(1024) # arbitrary number of outstanding connects allowed
	return s

def client_sock(addr,port):
	"standard client sock for my code: non blocking, connects to addr/port over TCP"
	log('connecting to ' + addr + ' ' + str(port))
	c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c.setblocking(False)
	try:
		c.connect((addr,port))
	except socket.error:
		pass
	return c

def test():
	port = 12346
	ps = PakServer(listen_sock(port),"testserver")
	pc0 = PakSender(client_sock('localhost',port))
	pc1 = PakSender(client_sock('localhost',port))
	ps.tick() # should see 2 accepts
	pc0.send_obj(PakClientMsg("0: hello port %i" % port))
	pc1.send_obj(PakClientMsg("1: hello port %i" % port))
	ps.tick()
	del ps
