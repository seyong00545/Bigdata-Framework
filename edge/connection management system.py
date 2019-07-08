# Import socket module 
import socket 
import zipfile
import threading
import time
import kafka
from kafka import KafkaConsumer, TopicPartition

class ConnectionWithCloud():
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.s = socket.socket()
		self.isStopped = False
		self.offset = 0
	
	def connect(self):
		try:
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.s.connect((self.host, self.port))
		except socket.error:
			print("Exception : connection error")
			time.sleep(5)
			self.connect()
	
	def setTransmitter(self):
		self.connect()

		self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
		tp = TopicPartition('purchaseData',0)
		self.consumer.assign([tp])
		if self.isStopped:
			#self.consumer.seek_to_beginning()
			self.consumer.seek(tp,self.offset-1)
			self.isStopped = False
		self.transmitterRun()
	
	def transmitterRun(self):
		for message in self.consumer:
			if message.value:
				try:
					data = str(message.value)
					self.s.send(data.encode())
					self.offset = self.consumer.position(kafka.TopicPartition('purchaseData',0))
					print("\noffset is", self.consumer.position(kafka.TopicPartition('purchaseData',0)))
				except socket.error:
					print("Exception : socket error in transmitterRun()")
					self.consumer.close()
					self.s.close()
					self.isStopped = True
					self.setTransmitter()
	
	def setReceiver(self):
		self.connect()
		self.modelName ="recommendModel.zip"
		self.receiverRun()
	
	def receiverRun(self):
		try:
			while True:
				message = "Update request"
				self.s.send(message.encode())
				path = "models/"+self.modelName
				zfile = zipfile.ZipFile(path,mode='w')
				with open(path,'wb') as f:
					while True:
						data = self.s.recv(1024)
						print("data = ",data)
						if not data:
							break
						f.write(data)
						break #can not break this while so I wrote
				break
			self.s.close() 		
		except socket.error:
			print("Exception : socket error in receiverRun()")
			self.s.close()
			self.setReceiver()
		
if __name__ == '__main__': 
	host = '220.67.124.122'
	port = 20000
	answer = ""

	dataTransmitter = ConnectionWithCloud(host, port) # kafka consumer
	modelReceiver = ConnectionWithCloud(host, port) # client
	
	transmitterThread = threading.Thread(target=dataTransmitter.setTransmitter)
	receiverThread = threading.Thread(target=dataTransmitter.setReceiver)
	
	transmitterThread.start()
	
	while True:
		#print("Do you want to receive a latest model?(Y/n): ")
		answer = input()
		if answer == "Y":
			receiverThread.start()
			receiverThread.join(3)
		answer = ""
	
	
	
	
	
	
	
	
