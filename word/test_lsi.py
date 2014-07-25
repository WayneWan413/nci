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

time1 = time.ctime()

dictionary = corpora.Dictionary.load('deerwester.dict')
corpus = corpora.MmCorpus('deerwester.mm')

tfidf = models.TfidfModel.load('deerwester.tfidf')
lda = models.ldamodel.LdaModel.load('deerwester.lda')


source_list_cmd = '/Users/liudaoming/xml/*.xml'
listfile=glob.glob(source_list_cmd)

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

i = 0
texts = []
titles = []
for filename in listfile:
    if i > 10000:
        break
    basename = os.path.basename(filename)
    reader = open(filename)
    
    xmldict = xmltodict.parse(reader.read())
    
    if xmldict['document']['header']['resultId'] == 'R0000':
        titledata = xmldict['document']['basicinfo']['title']
        articles = xmldict['document']['articles']['article']
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
                vec_lda = lda[vec_tfidf]
                print word_list
                print vec_lda
            i = i + 1
            
        except TypeError,e:
            i = i + 1
        except:
            raise
        if i % 1000 == 0:
            print i
    reader.close()


time2 = time.ctime()
print time1
print time2