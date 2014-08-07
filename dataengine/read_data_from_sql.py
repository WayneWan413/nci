#encoding=UTF-8
import pyodbc
import pymongo
import datetime
from datetime import *
from pymongo import MongoClient

MONGO_FACTOR_DB = 'factor'
MONGO_FACTOR_CONCEPTLIST = 'conceptlist'
MONGO_FACTOR_TOP_CONCEPT = 'topconcept'
MONGO_FACTOR_CONCEPT_INDEX = 'concept'

mongoClient = MongoClient()
mongoDB = mongoClient[MONGO_FACTOR_DB]
mongoConceptList = mongoDB[MONGO_FACTOR_CONCEPTLIST]
mongoTopConcept = mongoDB[MONGO_FACTOR_TOP_CONCEPT]
mongoConceptIndex = mongoDB[MONGO_FACTOR_CONCEPT_INDEX]

def idFromMongo(str):
    return(int(str))

def idToMongo(id):
    return('%04d' % id)

# Code following is not suitable for parallel computing
conceptDict = {}
conceptListReader = mongoConceptList.find()
for concept in conceptListReader:
    conceptDict[concept['conceptname']] = idFromMongo(concept['conceptid'])

if len(conceptDict) > 0:
	iNextConceptID = max(conceptDict.values()) + 1
else:
	iNextConceptID = 1

dictConceptByName = {}
dictConceptByDate = {}

sqlConnection = pyodbc.connect("Driver=SQLServer;Server=172.20.2.213;UID=sa;PWD=0okm)OKM;PORT=1433;DATABASE=factor;TDS_VERSION=8.0;")
cursor = sqlConnection.cursor()
cursor.execute('select * from ConceptAttra order by ConceptName,AttraDate desc')
rows = cursor.fetchall()
for row in rows:
    conceptName = row.ConceptName.decode('gbk')
    attraDate = row.AttraDate.date()
    attraValue = int(float(row.AttraValue) * 10000)
    insertTime = row.InsertTime
    if not conceptDict.has_key(conceptName):
        conceptDict[conceptName] = iNextConceptID
        dictMongoConcept = {'conceptname':conceptName,'conceptid':('%04d' % iNextConceptID),'insertdate':('%s' % datetime.now().date())}
#       factor.conceptlist
#   {'conceptname','conceptid','insertdate'}
        mongoConceptList.insert(dictMongoConcept)
        iNextConceptID = iNextConceptID + 1
        
    if dictConceptByName.has_key(conceptName):
        dictConceptByName[conceptName]['index'][attraDate] = attraValue
    else:
        dictConcept = {}
        dictConcept['conceptid'] = ('%04d' % conceptDict[conceptName])
        dictDateValue = {}
	dictDateValue[attraDate] = attraValue
        dictConcept['index'] = dictDateValue
        dictConceptByName[conceptName] = dictConcept
    
    if dictConceptByDate.has_key(attraDate):
        dictConceptByDate[attraDate][conceptName] = attraValue
    else:
        dictConcept = {}
        dictConcept[conceptName] = attraValue
        dictConceptByDate[attraDate] = dictConcept

cursor.execute('select * from ConceptToStockLatest order by ConceptName')
rows = cursor.fetchall()
for row in rows:
    conceptName = row.ConceptName.decode('gbk')
    stockName = row.Stock.decode('gbk')
    print '%s:%s' % (conceptName,stockName)
    stockCode = '600000'
    corrValue = row.CorValue
    if dictConceptByName.has_key(conceptName):
        if dictConceptByName[conceptName].has_key('stock_list'):
            dictConceptByName[conceptName]['stock_list'].append({'stockcode':stockCode,'stockname':stockName,'corr':('%0.0f' % corrValue)})
        else:
            dictConceptByName[conceptName]['stock_list'] = [{'stockcode':stockCode,'stockname':stockName,'corr':('%0.0f' % corrValue)}]

maxDate = max(dictConceptByDate.keys())
topConcept = []
latestConcept = dictConceptByDate[maxDate]

i = 1
for concept in sorted(latestConcept,latestConcept.get,reverse=True):
    topConcept.append({'conceptid':('%04d' % conceptDict[concept]),'concept':concept,'rank':('%d' % i),'value':('%d' % latestConcept[concept])})
    i = i + 1
mongoTopConcept.remove()

for concept in topConcept:
#   factor.topconcept
# {'conceptid','concept','rank','value'}
    mongoTopConcept.insert(concept)

mongoConceptIndex.remove()

for name,concept in dictConceptByName.items():
    oneConcept = {}
    oneConcept['concept'] = name
    oneConcept['conceptid'] = concept['conceptid']
    stocklist = []
    index = []
    statlist = []
    i = 0
    for time,value in sorted(concept['index'].items(),key=lambda d:d[0]):
        if i > 120:
            break
        index.append({'time':('%s' % time),'value':('%d' % value)})
        i = i + 1
    if len(index) > 6:
        statlist.append({'name':'热度','value':index[0]['value']})
        statlist.append({'name':'变化','value':'%d' % (int(index[0]['value']) - sum(map(lambda d:int(d['value']),index[1:6]))/5)})
        statlist.append({'name':'相关性','value': '0'})
    
    oneConcept['index'] = index
    oneConcept['statlist'] = statlist
    print name
    if concept.has_key('stock_list'):
        oneConcept['stocklist'] = concept['stock_list']
    else:
        oneConcept['stocklist'] = []
#   factor.concept
# {'concept','conceptid','index':[{'time','value'}],'statlist':[{'name','value'}],'stocklist':[{'stockcode','stockname','corr'}]}
    mongoConceptIndex.insert(oneConcept)
