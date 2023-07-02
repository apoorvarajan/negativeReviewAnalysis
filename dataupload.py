# Please dont run this file, The data is already uploaded to the db. We do not want duplicates
import gzip
import simplejson
from pymongo import MongoClient
mongo_uri="mongodb+srv://apoorvarajan532:Apoorva532@cluster0.hniu7nz.mongodb.net/?retryWrites=true&w=majority"
db_name="532project"
def parse(filename):
    f = gzip.open(filename, 'r')
    entry = {}
    for l in f:
        l = l.strip()
        colonPos = l.find(b':')
        if colonPos == -1:
            yield entry
            entry = {}
            continue
        eName = l[:colonPos]
        rest = l[colonPos+2:]
        eName1 = eName.decode('ascii')
        rest1 = rest.decode('ascii')
        entry[eName1] = rest1
    yield entry
data=[]
mongodb_client=MongoClient(mongo_uri)
print("Connectted to MongoDB Client")
db=mongodb_client[db_name]
collection_name=db['53']
for e in parse("./data/Arts.txt.gz"):
    a=simplejson.dumps(e)
    collection_name.insert_one(e)
    data.append(e)