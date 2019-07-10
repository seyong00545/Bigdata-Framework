from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
import time
import sys, os
#import threading

### Add python kafka producer
from kafka import KafkaProducer

class DataFusion:
	def __init__(self):
		self.weightArr = []
		self.distanceArr = []
		self.productDict = {}
		self.faceDict = {}
		self.tc = 0
		self.ttt = 0
		self.cplt = 1
		
	def handlerS(self, t,rdd):
		records = rdd.collect()
		if self.tc==0:
			self.ttt = time.time()
		for record in records:	
			result = record[1]
			data = result.split(",")
			if data[0] == "W" or data[0]=="D":
				self.setSdata(result)
			else:
				self.setRdata(result)

	
	def setSdata(self, data):
		dt = data.split(",") #dt[0]=sensorID dt[1]=count dt[2]=time
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
		dt = data.split(",")
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
				self.cplt += 1
				#print("@@@@@",time.time() - self.ttt,"@@@@@")
				#print("@@@@@       ",self.cplt,"        @@@@@")
			else:
				print("Invalid fusion")
				
			self.tc += 1
			if self.tc >500:
				print("Start to clean")
				self.check()
				self.tc = 0
			
		except KeyError:
			print("Not exist")

				

### Create a StremingContext with batch interval of 1sec
if __name__ == "__main__":
	
	producer = KafkaProducer(bootstrap_servers='localhost:9092')	

	sc = SparkContext(appName="Data Fusion edge side")
	ssc = StreamingContext(sc,1)
	sqlContext = SQLContext(sc)
	sc.setLogLevel('ERROR')
	df = DataFusion()

	### Create kafka streaming with argv(localhost:2181)
	zkQuorum = sys.argv[1]
	#sensorData
	#recognitionData
	zone1_sensor = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer1", {"WDData": 2})
	zone1_sensor.foreachRDD(df.handlerS)

	#zone1_imgrecognition = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer2", {"PFData": 2})
	#zone1_imgrecognition.foreachRDD(df.handlerR)
	
	### Start the computation
	ssc.start()

	### Wait for the computation to terminate
	ssc.awaitTermination()
	

