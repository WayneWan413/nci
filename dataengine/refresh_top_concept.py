#encoding=UTF-8
import pymongo
import datetime
import redis
import json
from datetime import *
from pymongo import MongoClient

MONGO_CLIENT = MongoClient()
MONGO_CONCEPT_DB = MONGO_CLIENT['factor']
MONGO_CONCEPT_COLLECTION = MONGO_CONCEPT_DB['topconcept']

listTopConceptFromMongo = MONGO_CONCEPT_COLLECTION.find()
listConcept = []
for reader in listTopConceptFromMongo:
    listConcept.append({'id':reader['conceptid'],'conceptid':reader['conceptid'],'concept':reader['concept'],'conceptname':reader['concept'],'rank':int(reader['rank']),'value':int(reader['value']),'conceptvalue':int(reader['value'])})

listConcept.sort(key=lambda d:d['rank'])

for i in range(0,len(listConcept)):
    concept = listConcept[i]
    print concept
    listConcept[i]['rank'] = '%d' % concept['rank']
    listConcept[i]['value'] = '%d' % concept['conceptvalue']
    listConcept[i]['conceptvalue'] = '%d' % concept['conceptvalue']

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('concept',json.dumps({'list':listConcept}))
