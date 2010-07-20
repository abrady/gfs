import cPickle
PAK_VER = 0

class Sender:
	"dumb packet switched class"
	def __init__(self,sock):
		self.sock = sock

	def _send_int(self, n):
		n = "%16i" % len(str)
		print "sending ", str, n
		s.sock.send(n)

	def send_obj(self, obj):
		# send header
		self._sendint(PAK_VER)
		p = cPickle.dumps(obj)
		self._sendint(len(p))
		self._sendint(p)
		s.sock.send(p)

class Receiver:
