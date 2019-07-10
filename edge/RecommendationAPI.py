from kafka import KafkaConsumer, KafkaProducer
from kafka import KafkaConsumer
import sys

producer = KafkaProducer(bootstrap_servers='220.67.124.122:9092')
consumer = KafkaConsumer('spark-ws',bootstrap_servers='220.67.124.122:9092')

producer.send('ws-spark', (sys.argv[1]).encode())
producer.flush()

for message in consumer:
	if message.value:	
		print("%s"%(message.value))
		sys.exit()
