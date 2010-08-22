""" helper package for sending object asynchronously over sockets
"""
import sys
import socket
import cPickle
import select
from log import log as _log, err as _err
import msg

PAK_VER = 20100720

# logging_enabled = True
logging_enabled = False 

def log(name,str):
	if(logging_enabled):
		_log("[%s] %s " % (name,str))

def err(name,str):
	_err("[%s] %s " % (name,str))

def can_recv(sock):
	'''helper for testing sock status.
	NOTE: not atomic, i.e. a return of true is not a guarantee that you can recv data'''
	rds,_,_ = select.select([sock],[],[],0)
	return len(rds) > 0

def can_send(sock):
	'''helper for testing sock status.
	NOTE: not atomic, i.e. a return of true is not a guarantee that you can send'''
	_,ws,_ = select.select([],[sock],[],0)
	return len(ws) > 0

def conn_refused(sock):
	err = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
	return err == 10061

def sock_err(sock):
	i = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
	if i != 0:
		return socket.errorTab[i]
	return None

class VersionMismatch(Exception):
	def __init__(self,value):
		self.value = value
	def __str__(self):
		return self.value

class PakSender:
	"""simple class for sending packet switched data .
	Use this along with PakReceiver to send objects over a network
	TODO: optimizations, version can be once per connection, for example
	object sends could be coalesced, etc.
	"""
	def __init__(self,sock,name):
		self.sock = sock
		self.objs = []
		self.name = name

	def _send_int(self, n):
		log(self.name, "send_int %i" % n)
		n = "%16i" % n
		self.sock.send(n)

	def _send_obj(self, o):
		"send object directly on link. fails if socket not ready"
		try:
			self._send_int(PAK_VER)
			log(self.name, "send_obj " + str(o))
			p = cPickle.dumps(o) # if it fails here, obj probably not pickle-able
			self._send_int(len(p))
			self.sock.send(p)
			return True
		except socket.error as e:
			err(self.name, "error " + str(e))
			return False

	def send_obj(self,o):
		"queues object for sending until tick"
		self.objs.append(o)

	def send_pending(self):
		return len(self.objs)

	def tick(self):
		n_sent = 0
		for o in self.objs:
			if not self._send_obj(o):
				log(self.name, "couldn't send object")
				break
			n_sent += 1
		self.objs = self.objs[n_sent:]
		log(self.name, "sent %i, %i remain" %(n_sent,len(self.objs)))
		return not sock_err(self.sock)


class PakReceiver:
	"""class for receiving packet switched network traffic
	"""
	def __init__(self,sock,name):
		self.sock = sock
		self.name = name

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
			log(self.name,"recv_obj: socket closed on other end")
			return None 
		if(ver != PAK_VER):
			raise VersionMismatch("recv_obj")
		n = self._recv_int()
		s = self.sock.recv(n)
		obj = cPickle.loads(s)
		log(self.name, "loading %s instance" % type(obj))
		return obj

	def can_recv(self):
		return can_recv(self.sock)

	
class PakComm(PakReceiver,PakSender):
	'''helper for combining a sender and receiver.
	Use this class for bideractional communication with a PakServer
	- relies on the PakSender tick function to pump send data
	'''
	def __init__(self,sock_or_addrinfo,name):
		self.sock = None
		if isinstance(sock_or_addrinfo,tuple):
			addr,port = sock_or_addrinfo
			self.sock = client_sock(addr,port)
		elif isinstance(sock_or_addrinfo,socket.socket):
			self.sock = sock_or_addrinfo
		self.objs = []
		self.name = name

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
		self.client_socks = {}
		self.name = name

	def tick(self, obj_handler=None):
		"""
		Check for any incoming connections, and listen for any objects
		from open connections.

		obj_handler -- optional callback function, if this is None, any received objects are invoked via the __call__ interface
		"""
		try:
			conn, addr = self.listen_sock.accept()
			self.log('conn from %s' % str(addr))
			if addr in self.client_socks:
				self.log("duplicate connection from %s" % str(addr))
			self.client_socks[addr] = conn
		except socket.error:
			pass # no conn

		if not self.client_socks:
			return
		
		rds,_,_ = select.select(self.client_socks.values(),[],[],0)

		if(len(rds)):
			self.log("reading %i sockets" % len(rds))
			
		for r in rds:
			recvr = PakReceiver(r,self.name)
			obj = recvr.recv_obj()
			if obj:
				self.log("recv " + str(obj))
				if(obj_handler):
					obj_handler(obj,r)
				else:
					obj(r)
			else:
				# client disconnect, drop it
				self.client_socks.pop(r.getpeername())
				r.close()

	def close_client(self,sock):
		"helper for closing a client by socket"
		self.log("close_client %s" % sock)
		try:
			self.log("removing sock")
			self.client_socks.pop(sock.getpeername())
			self.log("closing socket")
			sock.close()
		except:
			self.log("error closing client %s" % str(sock))

	def log(self, str):
		log(self.name, str)


class PakSenderTracker(object):
	"""helper class for pumping PakSenders
	- a PakSender doesn't send objects until its tick function, and something
	needs to call that tick function, so if you're running a server it is handy to have a couple of methods for tracking senders and making sure your objects get sent.
	"""
	def make_tracked_sender(self,sock_or_addrinfo, log_ctxt = ""):
		"create a sender that is tracked by this ChunkServer, and is removed once its send queue is empty"
		sender = PakSender(sock_or_addrinfo,"master:%s" % log_ctxt)
		self.senders.append(sender)
		return sender

	def send_obj_to(self, sock_or_addrinfo, obj, log_ctxt=""):
		"send the object to the destination"
		sender = self.make_tracked_sender(sock_or_addrinfo)
		sender.send_obj(obj)

	def tick_senders(self):
		for sender in self.senders[:]:
			self.log("ticking sender " + str(sender))
			sender.tick()
			if len(sender.objs) == 0:
				self.senders.remove(sender)


def listen_sock(port):
#	_log("listening on port %i" % port)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(('',port))
	s.setblocking(0)
	s.listen(1024) # arbitrary number of outstanding connects allowed
	return s

def client_sock(addr,port):
	"standard client sock for my code: non blocking, connects to addr/port over TCP"
#	_log('connecting to ' + addr + ' ' + str(port))
	c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	c.setblocking(False)
	errval = c.connect_ex((addr,port)) # since this is async, may get block error
	if errval != 0 and errval != 10035: # magic code for 'would block'
		return None
	return c

def test():
	port = 12346
	ps = PakServer(listen_sock(port),"testserver")
	pc0 = PakSender(client_sock('localhost',port),"testserver")
	pc1 = PakSender(client_sock('localhost',port),"testserver")
	ps.tick() # should see 2 accepts
	pc0.send_obj(PakClientMsg("0: hello port %i" % port))
	pc1.send_obj(PakClientMsg("1: hello port %i" % port))
	pc0.tick()
	pc1.tick()
	ps.tick()
	del ps
