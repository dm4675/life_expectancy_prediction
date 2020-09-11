# Life Expectancy Prediction - Big data Project

To predict the life expectancy of people in different countries using various factors like GDP, per capita income, mortality rate etc. Live generated data from Kafka streams is stored in MongoDB, which is read into a Spark data frame and a regression algorithm predict the life expectancy. The results are saved in Elastic Search to generate a Kibana dashboard.

## Pre-requisites:

	Python - pymongo
	MongoDB
	Kafka
	Elastic Search
	Kibana

## Execution

	Start MongoDB

		mkdir data/db
		mongod --dbpath data/db

	Start zoo-keeper

		bin/zookeeper-server-start.sh config/zookeeper.properties

	Start kafka-broker

		bin/kafka-server-start.sh config/server.properties

	Create a topic

		bin/kafka-topics.sh --create --topic test2 --bootstrap-server localhost:9092

	Start Elastic search

		./bin/elasticsearch

	Clone repo

		git clone https://github.com/swumar/life_expectancy_prediction.git
		cd life_expectancy_prediction/src

	Run the following:

		python mongodb_load.py   (loads training data in mongodb)
		python kafkaproducer.py  (generates test data and loads into topic)
		python kafkaconsumer.py  (consumes data from the topic and loads into mongodb)
		python prediciton.py


