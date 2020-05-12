from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

client = MongoClient('localhost',27017)
db = client['data']
collection = db['testdata']
collection.delete_many({})

consumer = KafkaConsumer('test2',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',consumer_timeout_ms=2000,
                         enable_auto_commit=True,group_id='my-group',value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    collection.insert_one(message)

consumer.close()
print("Kafka consumer successful")