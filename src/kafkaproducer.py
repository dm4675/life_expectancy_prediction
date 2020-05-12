import csv
from time import sleep
from json import dumps
from kafka import KafkaProducer

csvfile = open('predict_dataset.csv', 'r')
reader = csv.DictReader( csvfile )
header = ["country","year","status","life_expectancy","adult_mortality","infant_deaths","alcohol","percentage_expenditure","hepatitis_b","measles","bmi","under_five_deaths","polio","total_expenditure","diphtheria","aids","gdp","population", "thinness_1to19_years", "thinness_5to9_years","income","schooling"]

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

for each in reader:
    row = {}
    for field in header:
        row[field] = each[field]
    producer.send('test2', value=row)
    sleep(1)

print("Kafka producer successful")
