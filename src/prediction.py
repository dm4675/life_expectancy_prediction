from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("myApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Load data

traindata = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/data.traindata").load()
testdata = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/data.testdata").load()

#Clean data

traindata = traindata.select(traindata.adult_mortality.cast('float'),traindata.aids.cast('float'),traindata.alcohol.cast('float'),traindata.bmi.cast('float'),traindata.diphtheria.cast('float'),\
                   traindata.gdp.cast('float'),traindata.hepatitis_b.cast('float'),traindata.income.cast('float'),traindata.infant_deaths.cast('float'),traindata.measles.cast('float'),\
                   traindata.percentage_expenditure.cast('float'),traindata.polio.cast('float'),traindata.population.cast('float'),traindata.schooling.cast('float'),\
                   traindata.thinness_1to19_years.cast('float'),traindata.thinness_5to9_years.cast('float'),traindata.total_expenditure.cast('float'),\
                   traindata.under_five_deaths.cast('float'),'status',traindata.life_expectancy.cast('float').alias('label'))
traindata = traindata.na.drop()

testdata = testdata.select(testdata.adult_mortality.cast('float'),testdata.aids.cast('float'),testdata.alcohol.cast('float'),testdata.bmi.cast('float'),testdata.diphtheria.cast('float'),\
                   testdata.gdp.cast('float'),testdata.hepatitis_b.cast('float'),testdata.income.cast('float'),testdata.infant_deaths.cast('float'),testdata.measles.cast('float'),\
                   testdata.percentage_expenditure.cast('float'),testdata.polio.cast('float'),testdata.population.cast('float'),testdata.schooling.cast('float'),\
                   testdata.thinness_1to19_years.cast('float'),testdata.thinness_5to9_years.cast('float'),testdata.total_expenditure.cast('float'),\
                   testdata.under_five_deaths.cast('float'),'status',testdata.life_expectancy.cast('float').alias('label'))
testdata = testdata.na.drop()

#Feature data

assembler2 = VectorAssembler(inputCols=['gdp','population','income','adult_mortality','percentage_expenditure',\
                                        'total_expenditure','thinness_5to9_years','polio','aids','infant_deaths','bmi','alcohol'],outputCol="features")
train_data = assembler2.transform(traindata)
test_data = assembler2.transform(testdata)

#Evaluate model

lr = LinearRegression()
fit_model = lr.fit(train_data)
res = fit_model.summary
print("Error is:",res.rootMeanSquaredError)

#Predict

predictions = fit_model.transform(test_data)
predictions.select("prediction", "label", "features").show()