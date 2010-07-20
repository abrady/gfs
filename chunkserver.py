import socket
import gfs
import net
import master

def srv(settings):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((settings.MASTER_ADDR,settings.MASTER_CHUNKPORT))
	master_conn = net.Sender(s)
	master_conn.send_obj(master.ChunkMsg())

if __name__ == "__main__":
	chunkserver()
