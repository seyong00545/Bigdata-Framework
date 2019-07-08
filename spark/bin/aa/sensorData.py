from kafka import KafkaConsumer, KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers='localhost:9092')
#"W,933,1,1560085613",
msg   =["W,111,2,1560085614" , "W,213,1,1560085614",
	"D,111,2,1560085614" , "W,321,3,1560085614",
	"D,213,1,1560085615" , "D,321,3,1560085615",
	"W,411,1,1560085615" , "D,411,1,1560085615",
	"W,822,1,1560085616" , "D,822,1,1560085616",
	"W,923,1,1560085616" , "D,923,1,1560085617",
	"W,611,1,1560085617" , "D,611,1,1560085617",
	"W,1011,5,1560085618" , "W,531,3,1560085618",
	"D,1011,5,1560085618" , "D,531,3,1560085618",
	"D,713,3,1560085619" , "W,713,3,1560085619"]


#keyprice = raw_input("Enter the key and price :")
for x in range(1,501):
	for i in msg:
		print(i)
		producer.send('WDData',i.encode())
		#producer.send('input_hard',i.encode())		#hardcording
		producer.flush()
		time.sleep(0.2)
	time.sleep(0.5)
	"""if x%20==0:
		producer.send('WDData',("W,933,1,1560085613").encode())
		#producer.send('input_hard',("W,933,1,1560085613").encode())		#hardcording
		producer.flush()"""
