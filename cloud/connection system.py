# import socket programming library 
import socket 

# import thread module 
from _thread import *
import threading 

import zipfile

BUFFER_SIZE = 1024
print_lock = threading.Lock() 
alsVer = "als_1v.zip"
# thread fuction 
def threaded(c): 
	while True:
		# data received from client 
		data = (c.recv(BUFFER_SIZE)).decode()
		#print(data)
		if not data: 
			print('Bye')
			print_lock.release() 
			break
		
		if data == 'Release new version':
			global alsVer
			alsVer = data[1]
		elif data == 'Update request':
			global alsVer
			if data[1] != alsVer:
				print("Start file transfer")
				with open(alsVer,'rb') as f:
					l = f.read()
					c.send(l)
				print("Transfer complete")

			#else:c.send("You have a newest version model".encode())
		else:
			print(data)
			""" Use it to suit your purpose. """
		print("==============================================")
	# connection closed 
	c.close() 


def Main(): 
	host = "192.168.0.3" 
	port = 30000

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.bind((host, port)) 
	print("socket binded to post", port) 

	# put the socket into listening mode 
	s.listen(5) 
	print("socket is listening") 

	# a forever loop until client wants to exit 
	while True: 

		# establish connection with client 
		c, addr = s.accept() 

		# lock acquired by client 
		print_lock.acquire() 
		print('Connected to :', addr[0], ':', addr[1]) 

		# Start a new thread and return its identifier 
		start_new_thread(threaded, (c,)) 
	s.close() 


if __name__ == '__main__':
    Main()

