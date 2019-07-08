import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


#Add python kafka producer
from kafka import KafkaProducer

def check_value(val):
	x = int(val)
	if x>10:
		return "Fire"
	return "Safe"

#Receive data handler
#Check value and send to kafka
def myhandler(rdd):
	records = rdd.collect()	
	for record in records:	
		result = record[1]
		producer.send('restest', str(result))
		producer.flush()

#Create a StremingContext with batch interval of 1sec
if __name__ == "__main__":
	if len(sys.argv) != 3:
		sys.exit(-1)
	sparkConf = SparkConf()
	producer = KafkaProducer(bootstrap_servers='localhost:9092')	
	sc = SparkContext(appName="Test_Seyong", conf=sparkConf)
	sc.setLogLevel("WARN")

	ssc = StreamingContext(sc,1)

	#Create kafka streaming with argv(localhost:2181, test)
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	kvs.foreachRDD(myhandler)

	#Start the computation
	ssc.start()

	#Wait for the computation to terminate
	ssc.awaitTermination()
