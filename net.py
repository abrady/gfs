import cPickle
PAK_VER = 20100720

class VersionMismatch(Exception):
	def __init__(self,value):
		self.value = value
	def __str__(self):
		return self.value

class Sender:
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

class Receiver:
	def __init__(self,sock):
		self.sock = sock

	def _recv_int(self):
		s = self.sock.recv(16)
		return int(s)

	def recv_obj(self):
		ver = self._recv_int()
		if(ver != PAK_VER):
			raise VersionMismatch("recv_obj")
		n = self._recv_int()
		s = self.sock.recv(n)
		return cPickle.loads(s)
