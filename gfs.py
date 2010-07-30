import socket
import chunkserver
import master
import client
import net

try:
	import settings # Assumed to be in the same directory.
except ImportError:
	sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. This is required\n" % __file__)
	sys.exit(1)
	
if(settings.DEBUG):
	reload(settings)

def execute():
	if(settings.DEBUG):
		reload(settings)
		reload(chunkserver)
		reload(master)
		reload(client)
		reload(net)

	if(settings.TESTING):
		import thread
		thread.start_new_thread(chunkserver.srv,())
		thread.start_new_thread(master.srv,())
		client.test()


if __name__ == "__main__":
    execute()

# testing, remove
execute()
