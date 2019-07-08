from kafka import KafkaConsumer, KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers='localhost:9092')
#dt[0] = data type, dt[1] = location dt[2]=recognition result dt[3]=time
msg   =["P,1,kkokkal corn,1560085614","F,1,user1,1560085614",
	"P,2,LotteSand,1560085614","F,2,user2,1560085614",
	"P,3,Choco songi,1560085614","F,3,user3,1560085614",
	"P,4,Coffe,1560085615","F,4,user1,1560085615",
	"P,8,Swing chip, 1560085616","F,8,user3,1560085616",
	"P,9,Poca chips,1560085616","F,9,user2,1560085616",
	"P,6,Milk,1560085617","F,6,user1,1560085617",
	"P,10,LotteSand,1560085618","F,10,user3,1560085618",
	"P,5,Daije, 1560085618","F,5,user7,1560085618",
	"P,7,Orange juce,1560085619","F,7,user3,1560085619"]


#keyprice = raw_input("Enter the key and price :")
for x in range(500):
	for i in msg:
		print(i)
		producer.send('WDData',i.encode())
		#producer.send('input_hard',i.encode())		#hardcoding
		producer.flush()
		time.sleep(0.2)
	time.sleep(0.5)


