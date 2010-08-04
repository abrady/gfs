import sys

def log(str):
	sys.stderr.write("LOG\t" + str + "\n")

def err(str):
	sys.stderr.write("ERR\t" + str + "\n")
