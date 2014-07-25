#encoding=utf-8
import os
import sys
import glob
import xmltodict
import jieba
import codecs
import time
from gensim import corpora, models, similarities
import pymongo
from pymongo import MongoClient
from os.path import exists
import redis
import json

time1 = time.ctime()

dictionary = corpora.Dictionary.load('deerwester.dict')
corpus = corpora.MmCorpus('deerwester.mm')

tfidf = models.TfidfModel.load('deerwester.tfidf')
lda = models.ldamodel.LdaModel.load('deerwester.lda')

dic_word = {}
for word,id in dictionary.token2id.items():
    dic_word[str(id)] = word

topic_words = []
i = 0
for topics in lda.show_topics(200,20,formatted=False):
    onetopic = {}
    word_list = []    
    for topicword in topics:
        word_list.append({'word':dic_word[topicword[1]] ,'freq':'%.2f' % (topicword[0] * 100)})
    onetopic['id'] = i
    onetopic['word_list'] = word_list
    topic_words.append(onetopic)
    i = i + 1

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('topicwords',json.dumps(topic_words))

source_list_cmd = '/Users/liudaoming/xml/*.xml'
listfile=glob.glob(source_list_cmd)

for j in range(1,len(topic_words)+1):
    topic_words[j-1]['docs'] = []
i = 0


client = MongoClient()
db = client['word']
collection = db['keywords']
jieba.initialize()
jieba.set_dictionary('dict.txt.big')
reader = collection.find({'user':1,'cut':1})
keyword_list = {}
for word in reader:
    jieba.add_word(word['name'],word['weight'],word['tag'])
    keyword_list[word['name']] = 1
    
for filename in listfile:
    basename = os.path.basename(filename)
    reader = open(filename)
    xmldict = xmltodict.parse(reader.read())
    
    if xmldict['document']['header']['resultId'] == 'R0000':
        titledata = xmldict['document']['basicinfo']['title']
        articles = xmldict['document']['articles']['article']
        url = xmldict['document']['basicinfo']['url']
        try:
            postdata = articles['topicinfo']['postdata']
            cutData = jieba.tokenize(postdata,mode='default',HMM=False)
            word_list = []
            for tk in cutData:
                if keyword_list.has_key(tk[0]):
                    word_list.append(tk[0])
            if len(word_list) > 0:
                vec_bow = dictionary.doc2bow(word_list)
                vec_tfidf = tfidf[vec_bow]
                vec_lda = lda[vec_bow]
                for id,corr in vec_lda:
                    if corr > 0.3:
                        topic_words[id]['docs'].append({'title':titledata,'postdata':postdata,'corr':'%.2f' % (corr * 100),'url':url})
            i = i + 1
            
        except TypeError,e:
            i = i + 1
        except:
            raise
        if i % 1000 == 0:
            print i
    reader.close()
    
for j in range(1,len(topic_words)+1):
    topicstr = 'topic'+str(j-1)
    r.set(topicstr,json.dumps(topic_words[j-1]))
    
