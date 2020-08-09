import pymongo
import redis
import psycopg2
import os
import numpy as np
#create_table creates a table in postgres database
def create_table():
    conn = psycopg2.connect(database="postgres", user = "postgres", password = "1234", host = "localhost", port = "5432")

    cur = conn.cursor()
    #print ( conn.get_dsn_parameters(),"\n")
    cur.execute('''CREATE TABLE Ads
      (ID  SERIAL PRIMARY KEY,
      recognition_time timestamp DEFAULT now(),
      advertiser_name          TEXT    NOT NULL,
      ad_name          TEXT    NOT NULL,
      ad_frame      CHAR(200)  NOT NULL,
      duration      INT       NOT NULL,
      fps         INT     NOT NULL,
      stream_frame      CHAR(200)  NOT NULL
      );''')
    conn.commit()
    conn.close()
#red function creates a redis database and set the fps value ,we pass a fps value we want then it return that fps value   
def red(f):
  r = redis.StrictRedis(
      host='127.0.0.1',
      port=6379,
      db=0
        )
  r.set("fps60",60)
  r.set("fps30",30)
  fps=r.get(f)
  return int(fps)
#mongo function creates a collection and inserts all ads data in tha collection
def mongo(ads,fdur):
  myclient = pymongo.MongoClient("mongodb://localhost:27017/")
  mydb = myclient["mydatabase"]
  mycol = mydb["adcuratio"]
  mylist=[]
  for i in range(0,len(ads)):
    s=ads[i]
    rec=s.split('_')
    advertiser_name=rec[0]
    ad_name=rec[1]
    dict={"ads":s,"advertiser_name":advertiser_name,"ad_name":ad_name,"duration":fdur[i]}
    mylist.append(dict)
  mycol.insert_many(mylist)
#query function finds the row in table corresponding to a query  
def query(dict):
  myclient = pymongo.MongoClient("mongodb://localhost:27017/")
  mydb = myclient["mydatabase"]
  mycol = mydb["adcuratio"]
  mydoc = mycol.find_one(dict)
  return mydoc
  
#push function inserts the data in the postgres table
def push(x,y,fps,hash1,doc):
  conn = psycopg2.connect(database="postgres", user = "postgres", password = "1234", host = "localhost", port = "5432")
  cur = conn.cursor()
  ad_frame=hash1[x]
  stream_frame =y
  ad_name=doc.get("ad_name")
  advertiser_name=doc.get("advertiser_name")
  duration=doc.get("duration")
  cur.execute("INSERT INTO ads(advertiser_name,ad_name,ad_frame,duration,fps,stream_frame) VALUES (%s, %s, %s, %s, %s, %s);",
	(advertiser_name,ad_name,ad_frame,duration,fps,stream_frame))
  conn.commit()
  conn.close() 
#mydict = { "name": "John", "address": "Highway 37" }
#print(myclient.list_database_names())
#x = mycol.insert_one(mydict)
#print(mydb.list_collection_names())
