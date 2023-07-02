from pymongo import MongoClient
import pandas as pd
from pyspark.sql import SparkSession
from afinn import Afinn
from pyspark.sql.functions import col, split,lower
from nltk.tokenize import sent_tokenize , word_tokenize
from pyspark.ml.feature import Word2Vec
afinn=Afinn(language='en')
mongo_uri="mongodb+srv://apoorvarajan532:Apoorva532@cluster0.hniu7nz.mongodb.net/?retryWrites=true&w=majority"
db_name="532project"
mongodb_client=MongoClient(mongo_uri)
db=mongodb_client[db_name]
collection_name=db['53']
cur=collection_name.find({})
data=[]
for i in cur:
    data.append(i)
alldata=pd.DataFrame(data)
intermediate_data=alldata.drop(['product/price','review/userId','review/profileName','review/time','_id'],axis=1).dropna()
intermediate_data.set_index('product/productId')
intermediate_data['sentiment_Score']=intermediate_data['review/text'].apply(lambda x: afinn.score(x))
print(intermediate_data.columns)
#intermediate_data['product/category'] = 'Arts'
#intermediate_data.to_json(r'./data/arts.json', orient='records')
#print(intermediate_data[intermediate_data['product/productId']=='B00064C0IU'])
spark=SparkSession.builder \
    .master("local[1]") \
    .appName("SparkAmazonReview.com") \
    .getOrCreate()
sc = spark.sparkContext
rdd=sc.parallelize(intermediate_data)
sparkdf=spark.createDataFrame(intermediate_data)
sparkdf=sparkdf.withColumn("text_splitted", split(lower(col("review/text")), " "))
print(sparkdf.show())
model1 = Word2Vec(vectorSize=100, minCount=0, maxIter=100, inputCol="text_splitted", outputCol="features")
model = model1.fit(sparkdf)
result = model.transform(sparkdf)
print(result.select("features").rdd.flatMap(lambda x: x).collect())
