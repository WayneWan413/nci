#encoding=utf-8
import os
import sys
import glob
import time
import xmltodict
import json
import jieba
import pymongo
from pymongo import MongoClient
from os.path import exists

# 1. Load word from word.keywords
# 2. Load concept word from factor.conceptlist
# 3. Init jieba cut engine and load the keywords
# 4. Load file from src_directory
# 5. Convert xml to dict
# 6. Get items and cut data, calc the news-concept correlations
# 7. Move file to dest directory
# 8. Save data to mongo

MONGO_CLIENT = MongoClient(‘172.20.2.207’)
MONGO_WORD_DB = MONGO_CLIENT['word']
MONGO_WORD_COLLECTION = MONGO_WORD_DB['keywords']
MONGO_DOC_DB = MONGO_CLIENT['doc']
MONGO_DOC_COLLECTION = MONGO_DOC_DB['xmls']
MONGO_CONCEPT_DB = MONGO_CLIENT['factor']
MONGO_CONCEPT_COLLECTION = MONGO_CONCEPT_DB['conceptlist']

XML_SOURCE_FILE = '/home/daoming/data/*.xml'
XML_DEST_DIR = '/home/daoming/xml/'

def jiebaToList(data):
    wordList = []
    for tk in data:
        wordList.append({'word':tk[0].decode('gbk'),'pos':tk[1]})
    return wordList

jieba.initialize()
jieba.set_dictionary('dict.txt.big')
reader = MONG_WORD_COLLECTION.find({'user':1,'cut':1})
for word in reader:
    jieba.add_word(word['name'],word['weight'],word['tag'])

dictConceptList = {}
reader = MONGO_CONCEPT_COLLECTION.find()
for word in reader:
    jieba.add_word(word['conceptname'],999,'NCI')
    dictConceptList[word['conceptname'].decode('gbk')] = word['conceptid']

filenameList=glob.glob(XML_SOURCE_FILE)
fileList = []
i = 0

for filename in filenameList:
    basename = os.path.basename(filename)
    reader = open(filename)
    xmldict = xmltodict.parse(reader.read())
    reader.close()
    if xmldict['document']['header']['resultId'] == 'R0000':
        try:
            seqid = xmldict['document']['header']['sequenceId']
            filedate = xmldict['document']['header']['timestamp'][0:8]
            article = xmldict['document']['articles']['article']
            titledata = xmldict['document']['basicinfo']['title']
            postdata = articles['topicinfo']['postdata']
            cutTitle = jieba.tokenize(titledata,mode='default')
            cutData = jieba.tokenize(postdata,mode='default')
            posttime = xmldict['document']['articles']['article']['topicinfo']['posttime']
            tmpDir = XML_DEST_FILE + filedate
            cutResult = {'result':'1','posttime':posttime,'cuttitle':cutTitle,'cutdata':cutData}
            dictCutData = jiebaToList(cutData)
            dictCutTitle = jiebaToList(cutTitle)
            dictConceptStat = {}
            listConceptWord = []
            for word in dictCutTitle:
                if dictConceptStat.has_key(word['word']):
                    dictConceptStat[word['word']] = dictConceptStat[word['word']] + 2
                else:
                    if dictConceptList.has_key(word['word']):
                        dictConceptStat[word['word']] = 2
            
            for word in dictCutData:
                if dictConceptStat.has_key(word['word']):
                    dictConceptStat[word['word']] = dictConceptStat[word['word']] + 1
                else:
                    if dictConceptList.has_key(word['word']):
                        dictConceptStat[word['word']] = 1
            if len(dictConceptStat) > 0 and len(dictConceptStat) < 4:
                s = float(sum(dictConceptStat.values()))
                for word,value in sorted(dictConceptStat.items(),key=lambda d:d[1],reverse=True):
                    listConceptWord.append('conceptname':word,'conceptid':dictConceptList[word],'corr':float(value)/s)
                cutResult['concept'] = '1'
                cutResult['conceptlist'] = []
                for concept in listConceptWord:
                    cutResult['conceptlist'].append({'conceptid':concept['conceptid'],'id':concept['conceptid'],'name':concept['conceptname'],'conceptname':concept['conceptname'],'corr':'%.2f' % concept['corr']})
            
        except:
            tmpDir = XML_DEST_FILE + 'error/' + filedate
            cutResult = {'result':'0'}
        i = i + 1
        if i % 1000 == 0:
            print i
        destFile = tmpDir + '/' + basename
        fileList.append({'seqid':seqid,'cutresult':cutResult,'xml':xmldict,'srcfile':filename,'destfile':destFile})
    if not exists(tmpDir):
        os.mkdir(tmpDir)
    
#    os.rename(filename,destFile)

fileList = sorted(fileList,key=lambda files:(files['posttime']),reverse=True)
for doc in fileList:
    try:
        os.rename(doc['srcfile'],doc['destfile'])
        doc_collection.insert(doc)
    except:
        continue
