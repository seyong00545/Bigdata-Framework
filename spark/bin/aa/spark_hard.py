import threading
from threading import Lock
from kafka import KafkaConsumer
import time
import sys, os
#import threading

### Add python kafka producer
from kafka import KafkaProducer

count = 1
lock = Lock()

class DataFusion:
	def __init__(self):
		self.task = []
		self.weightArr = []
		self.distanceArr = []
		self.productDict = {}
		self.faceDict = {}
		self.tc = 0
	
	def handler(self,task):
		print("DataFusion start")
		while(True):
			if len(task) > 0:
				data = task[0]
				stype = data[0]
				if stype == "P" or stype =="F":
					self.setRdata(data)
					del task[task.index(data)]
				elif stype == "W" or stype =="D":
					self.setSdata(data)
					del task[task.index(data)]		

	
	def setSdata(self, data):
		dt = data
		stype, sensorID , count ,time = dt[0], dt[1], dt[2], dt[3]
		item = [[int(time)-1, sensorID, count],
			[int(time), sensorID, count],
			[int(time)+1, sensorID, count]]

		#print("wlist = ",self.weightArr,"\ndlist = ",self.distanceArr)
		#print("------------------------------------------------------")
		if stype =='W':
			if item[0] in self.distanceArr or item[1] in self.distanceArr or item[2] in self.distanceArr:
				for i in item:
					if i in self.distanceArr:
						self.distanceArr.remove(i)
				self.fuse(item[1])
				
			else:
				self.weightArr.append(item[1])
			
		elif stype =='D':
			if item[0] in self.weightArr or item[1] in self.weightArr or item[2] in self.weightArr:
				for i in item:
					if i in self.weightArr:	
						self.weightArr.remove(i)
				self.fuse(item[1])
			else:
				self.distanceArr.append(item[1])
	def setRdata(self,data):
		dt = data
		#dt[0] = data type, dt[1] = location dt[2]=recognition result dt[3]=time
		data_type, location, rcgn , time = dt[0], dt[1], dt[2], dt[3]
		
		if data_type == "P":
			self.productDict[location] = time+","+rcgn
		elif data_type =="F":
			self.faceDict[location] = time + "," + rcgn
			
	def check(self):
		#time = int(time.time())
		time = 1560085619
		wlen = len(self.weightArr)
		dlen = len(self.distanceArr)
		if wlen != 0:
			if (time-self.weightArr[0][0]) > 5:
				self.clean(self.weightArr,time)
			else:
				pass
		if dlen != 0:
			if (time-self.distanceArr[0][0]) >5:
				self.clean(self.distanceArr, time)
			else:
				pass
	
	def clean(self, arr, time):
		print("Cleaning")
		print("arr = ",arr,"time = ",time)
		for x in arr:
			if time-x[0] > 5:
				arr.remove(x)
			else:
				break

	
	def fuse(self, item):
		#delete the value and then return result to webserver through kafka
		try:
			global count
			num = item[1][:-2]
			p = (self.productDict[num]).split(",")
			ptime, productname = int(p[0]), p[1]
			
			f = (self.faceDict[num]).split(",")
			ftime, user = int(f[0]), f[1]
			print("User : "+user+" Product name : " + productname +" Product location : "+str(item[1])+" Number of product : "+str(item[2]))
			t = item[0]
			if abs(t-ptime)<3 and abs(t-ftime)<3:
				producer.send('result', ("User : "+user+" Product name : " + productname +" Product location : "+str(item[1])+" Number of product : "+str(item[2])).encode())
				producer.flush()
				lock.acquire()
				count += 1
				#print("\n@@@@@@"+str(time.time()-ttt)+"@@@@@@\n@@@@@@@"+str(count))
				lock.release()
			else:
				print("Invalid fusion")
				
			self.tc += 1
			if self.tc >200:
				print("Start to clean")
				self.check()
				self.tc = 0
			
		except KeyError:
			print("Not exist")

class Consumer:
	def __init__(self):
		self.consumer = KafkaConsumer('input_hard',bootstrap_servers='localhost:9092')		

	def consume(self):
		print("consumer start")
		for message in self.consumer:
			data = (message.value).split(",")
			stype, sensorID , count ,t = data[0], data[1], data[2], data[3]
			#print("in cosumer   ",data)
			#task1.append(data)
			
			"""
			if stype == "P" or stype == "F":
				if int(sensorID)< 6:
					df1.task.append(data)
				else:
					df2.task.append(data)

			elif stype == "W" or stype =="D":
				sensorID = sensorID[:-2]
				if int(sensorID)< 6:
					df1.task.append(data)
				else:
					df1.task.append(data)"""
					
			
			
			if stype == "P" or stype == "F":
				if 1 <= int(sensorID) and int(sensorID) < 4:
					task1.append(data)
				elif 4 <= int(sensorID) and int(sensorID) < 6:
					task2.append(data)
				elif 6 <= int(sensorID) and int(sensorID) < 9:
					task3.append(data)
				else:
					task4.append(data)

			elif stype == "W" or stype =="D":
				sensorID = sensorID[:-2]
				if 1 <= int(sensorID) and int(sensorID) < 4:
					task1.append(data)
				elif 4 <= int(sensorID) and int(sensorID) < 6:
					task2.append(data)
				elif 6 <= int(sensorI1;2cD) and int(sensorID) < 9:
					task3.append(data)
				else:
					task4.append(data)
					
### Create a StremingContext with batch interval of 1sec
if __name__ == "__main__":
	
	producer = KafkaProducer(bootstrap_servers='localhost:9092')	
	#consumer = KafkaConsumer('hard_input',bootstrap_servers='localhost:9092')

	df1 = DataFusion()
	df2 = DataFusion()
	df3 = DataFusion()
	df4 = DataFusion()
	consumer = Consumer()
	
	task1,task2,task3,task4 = [],[],[],[]
	
	t1 = threading.Thread(target=df1.handler)
	t2 = threading.Thread(target=df2.handler)
	t3 = threading.Thread(target=df3.handler, args=(task3,))
	t4 = threading.Thread(target=df4.handler, args=(task4,))
	t5 = threading.Thread(target=consumer.consume)
	
	t1.start()
	t2.start()
	t3.start()
	t4.start()
	t5.start()
	ttt = time.time()
	cplt = 1

