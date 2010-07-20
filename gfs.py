import socket
import chunkserver
import master
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
		reload(net)

	if(settings.TESTING):
		import thread
		thread.start_new_thread(chunkserver.srv,(settings,))
		
	master.srv(settings)


if __name__ == "__main__":
    execute()

# testing, remove
execute()
