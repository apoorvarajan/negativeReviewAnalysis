
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper,expr,lit,transform
from afinn import Afinn
afinn=Afinn(language='en')
from pymongo import MongoClient
mongo_uri="mongodb+srv://apoorvarajan532:Apoorva532@cluster0.hniu7nz.mongodb.net/?retryWrites=true&w=majority"
db_name="532project"
mongodb_client=MongoClient(mongo_uri)
print("Connectted to MongoDB Client")
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
print(sparkdf.show())