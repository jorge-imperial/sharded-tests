from pymongo import MongoClient, errors

from datetime import datetime

#client = MongoClient("mongodb://m17:27000,m18:27000,m19:27000")
#client = MongoClient("mongodb://domo:27017")
client = MongoClient( "mongodb+srv://user:XXXXXXX@cluster0-om7f7.mongodb.net/test?retryWrites=true&w=majority" )

db = client['test']
collection = db.get_collection('test001')

for i in range(1, 100000):
    now = datetime.now()
    r = collection.update_many({'_id': {'$in': [i*1, i*3, i*5, i*7]}}, {'$set': {'ts': now}})
    print('updated {},{},{},{}: match {}  modified {}'.format(i*1, i*3, i*5, i*7, r.matched_count, r.modified_count))
