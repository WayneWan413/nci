#encoding=UTF-8
import pymongo
import time
import datetime
import redis
import json
from pymongo import MongoClient

MONGO_CLIENT = MongoClient()
MONGO_CONCEPT_DB = MONGO_CLIENT['factor']
MONGO_CONCEPT_COLLECTION = MONGO_CONCEPT_DB['concept']
MONGO_NEWS_DB = MONGO_CLIENT['doc']
MONGO_NEWS_COLLECTION = MONGO_NEWS_DB['xmls']

listNewsFromMongo = MONGO_NEWS_COLLECTION.find({'cutresult.concept':'1'}).sort('cutresult.posttime',pymongo.DESCENDING).limit(200)
listNews = []
i = 1
for news in listNewsFromMongo:
    oneNews = {'id':'%05d' % i,'newsid':'%05d' % i,'posttime':int(news['cutresult']['posttime']),
        'source':news['xml']['document']['basicinfo']['url'],
        'title':news['xml']['document']['basicinfo']['title'],
        'sitename':news['xml']['document']['basicinfo']['sitename'],
        'abstract':news['xml']['document']['articles']['article']['topicinfo']['postdata'][0:51]}
    listConcept = []
    for concept in news['cutresult']['conceptlist']:
        oneConcept = {'corr':float(concept['corr']),
            'conceptid':concept['conceptid'],
            'id':concept['id'],
            'concept':concept['conceptname'],
            'conceptname':concept['conceptname']}
        oneConceptFromMongo = MONGO_CONCEPT_COLLECTION.find({'conceptid':oneConcept['conceptid']})
        statlist = []
        for cl in oneConceptFromMongo:
            if len(cl['statlist']) > 0:
                statlist = cl['statlist']
                statlist[2]['value'] = '%.0f' % (oneConcept['corr'] * 100)
            else:
                statlist.append({'name':'热度','value':'0'})
                statlist.append({'name':'变化','value':'0'})
                statlist.append({'name':'相关性','value': '0'})
        oneConcept['statlist'] = statlist
        listConcept.append(oneConcept)
        
    oneNews['posttime'] = (datetime.datetime.fromtimestamp(oneNews['posttime'])).strftime('%Y-%m-%d %H:%I:%S')
    oneNews['conceptlist'] = listConcept
    listNews.append(oneNews)
    i = i + 1

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('news',json.dumps({'list':listNews,'isEnd':'true'}))

    
    