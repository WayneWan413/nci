#encoding=utf-8
import os
import sys
import glob
import xmltodict
import jieba
import codecs
import pymongo
from pymongo import MongoClient
from os.path import exists

client = MongoClient()
db = client['word']
collection = db['keywords']
doc_word_collection = db['doc_word']
jieba.initialize()
jieba.set_dictionary('dict.txt.big')
reader = collection.find({'user':1,'cut':1})

for word in reader:
    jieba.add_word(word['name'],word['weight'],word['tag'])
    
source_list_cmd = '~/xml/*.xml'
dest_dir = '~/data/'

listfile=glob.glob(source_list_cmd)

for filename in listfile:
    basename = os.path.basename(filename)
    reader = open(filename)
    
    xmldict = xmltodict.parse(reader.read())
    
    if xmldict['document']['header']['resultId'] == 'R0000':
        titledata = xmldict['document']['basicinfo']['title']
        articles = xmldict['document']['articles']['article']
        
        try:
            postdata = articles['topicinfo']['postdata']
            cutTitle = jieba.tokenize(titledata,mode='default',HMM=False)
            cutData = jieba.tokenize(postdata,mode='default',HMM=False)
        
            cutResult = {}
            word_list = {}
            for tk in cutTitle:
                if word_list.has_key(tk[0]):
                    word_list[tk[0]].append(tk[1])
                else:
                    word_list[tk[0]] = [tk[1]]
            word_pair = []
            for pair in word_list:
                item = {'word' : pair,
                        'pos'  : word_list[pair]}
                word_pair.append(item)
            cutResult['titlelist'] = word_pair
        
            word_list = {}
            for tk in cutData:
                if word_list.has_key(tk[0]):
                    word_list[tk[0]].append(tk[1])
                else:
                    word_list[tk[0]] = [tk[1]]
            word_pair = []
            for pair in word_list:
                item = {'word' : pair,
                        'pos'  : word_list[pair]}
                word_pair.append(item)
            cutResult['wordlist'] = word_pair
        
            xmldict['cutResult'] = cutResult
            doc_word_collection.insert(xmldict)
            
            filedate = xmldict['document']['header']['timestamp'][0:8]
            tmpDir = dest_dir + filedate
            
            
        except:
            tmpDir = dest_dir + 'error/' + filedate
            print '==================ERROR==============='
        destFile = tmpDir + '/' + basename
    if not exists(tmpDir):
        os.mkdir(tmpDir)
    print filename
    os.rename(filename,destFile)

    

client.close()