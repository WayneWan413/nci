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

listConceptFromMongo = MONGO_CONCEPT_COLLECTION.find()

for reader in listConceptFromMongo:
    listConcept = []
    conceptid = reader['conceptid']
    listConceptNews = []
    listConceptNewsFromMongo = MONGO_NEWS_COLLECTION.find({'cutresult.conceptlist.conceptid':conceptid})
    for news in listConceptNewsFromMongo:
        for concept in news['cutresult']['conceptlist']:
            if concept['conceptid'] == conceptid:
                listConceptNews.append({'corr':float(concept['corr']),'posttime':int(news['cutresult']['posttime']),
                    'source':news['xml']['document']['basicinfo']['url'],
                    'title':news['xml']['document']['basicinfo']['title'],
                    'sitename':news['xml']['document']['basicinfo']['sitename'],
                    'abstract':news['xml']['document']['articles']['article']['topicinfo']['postdata'][0:51]})
    
    stocklist = reader['stocklist']
    index = reader['index']
    statlist = reader['statlist']
    
    if len(statlist) < 1:
        statlist.append({'name':'热度','value':'0'})
        statlist.append({'name':'变化','value':'0'})
        statlist.append({'name':'相关性','value': '0'})
    
    if len(listConceptNews) > 0:
        listConceptNews.sort(key=lambda d:d['posttime'],reverse=True)
        strTime = (datetime.datetime.fromtimestamp(listConceptNews[0]['posttime'])).strftime('%Y-%m-%d %H:%I:%S')
        source = listConceptNews[0]['source']
        title = listConceptNews[0]['title']
        sitename = listConceptNews[0]['sitename']
        abstract = listConceptNews[0]['abstract']
        statlist[2]['value'] = '%.0f' % (listConceptNews[0]['corr'] * 100)
        
    else:
        strTime = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%I:%S')
        source = 'NA'
        title = 'NA'
        sitename = 'NA'
        abstract = 'NA'
    listNews = []
    listNews.append({'posttime':strTime,
        'source':source,
        'title':title,
        'sitename':sitename,
        'abstract':abstract})
    listConcept.append({'id':conceptid,'conceptid':conceptid,'concept':reader['concept'],'conceptname':reader['concept'],
        'statlist':statlist,'index':index,'stocklist':stocklist,
        'news':listNews
        })
    
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    redisKeyname = 'concept%s' % conceptid
    r.set(redisKeyname,json.dumps({'list':listConcept}))
        