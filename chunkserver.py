import socket
import gfs


def srv(settings):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((settings.MASTER_ADDR,settings.MASTER_CHUNKPORT))
	s.send('Hello, world')

if __name__ == "__main__":
	chunkserver()
