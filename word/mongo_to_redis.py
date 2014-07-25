#endcoding=utf-8
import web
import pymongo
import json
import redis
from pymongo import MongoClient

client = MongoClient()
db = client['word']

news_collection = db['doc_word']

retList = []
result = news_collection.find().sort('document.header.timestamp',pymongo.DESCENDING).limit(200)
for doc in result:
    tmpNews = {}
    strTime = doc['document']['header']['timestamp']
    tmpNews['date'] = '%s-%s-%s' % (strTime[0:4],strTime[4:6],strTime[6:8])
    tmpNews['timestamp'] = '%s:%s:%s' % (strTime[8:10],strTime[10:12],strTime[12:14])
    tmpNews['title'] = doc['document']['basicinfo']['title']
    tmpNews['url'] = doc['document']['basicinfo']['url']
    tmpNews['source'] = doc['document']['basicinfo']['sitename']
    retList.append(tmpNews)

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('docs',json.dumps(retList))

client.close()
