from pymongo import MongoClient
import csv

client = MongoClient('localhost',27017)
db = client['data']
collection = db['traindata']


csvfile = open('dataset.csv', 'r')
reader = csv.DictReader( csvfile )
header = ["country","year","status","life_expectancy","adult_mortality","infant_deaths","alcohol","percentage_expenditure","hepatitis_b","measles","bmi","under_five_deaths","polio","total_expenditure","diphtheria","aids","gdp","population", "thinness_1to19_years", "thinness_5to9_years","income","schooling"]

data = []
for each in reader:
    row = {}
    for field in header:
        row[field] = each[field]
    data.append(row)

collection.delete_many({})
collection.insert_many(data)

print("Train data load successful")
