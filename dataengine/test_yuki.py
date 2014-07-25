#encoding=utf-8
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client['db']
collection = db['ConceptAttra']

reader = collection.find()
for read in reader:
	print read['ConceptName']
