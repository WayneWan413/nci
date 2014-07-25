#encoding=utf-8
import redis
reader = open('conceptlist.json')
data = reader.read()
r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('concept',data)
reader.close()
