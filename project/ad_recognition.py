from imutils import paths
import sys
import cv2
import os
import time 
import multiprocessing
import redis
import numpy as np
import pymongo
from database import red,create_table,mongo,push,query
import psycopg2
start=time.perf_counter()
dir1='E:\\adcuratio\\images\\project_stream'
dir2='E:\\adcuratio\\images\\project_ads'
os.chdir(dir1)
stream = os.listdir()
os.chdir(dir2)
ads=os.listdir()
#ads stores the list of ads folders and stream stores stores the list of stream images
#adf stores the first image of each  ad folder
adf=[]
size=[]

for i in ads:
	os.chdir('E:\\adcuratio\\images\\project_ads\\' + i)
	adf.append(os.listdir()[0])
	size.append(len(os.listdir()))

os.chdir('E:\\adcuratio')

def dhash(image, hashSize=8):
	# resize the input image, adding a single column (width) so we
	# can compute the horizontal gradient
	resized = cv2.resize(image, (hashSize + 1, hashSize))

	# compute the (relative) horizontal gradient between adjacent
	# column pixels
	diff = resized[:, 1:] > resized[:, :-1]
	# convert the difference image to a hash
	return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])


res1=[]
res2=[]
# res1 list stores the path of all stream images(frames)
## res2 list stores the path of all ad images(frames)
for i in range(0,len(stream)):   
    res1.append("E:\\adcuratio\\images\\project_stream\\"+stream[i])
for j in range(0,len(ads)):  
	res2.append("E:\\adcuratio\\images\\project_ads\\"+ads[j]+"\\"+adf[j])    

#hash1 dictionary stores the hash values of ad images
hash1={}

for p in res2:
	# load the image from disk
	image = cv2.imread(p)
	if image is None:
		continue
	# convert the image to grayscale and compute the hash
	image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
	imageHash = dhash(image)
	hash1[imageHash] = p
#path is the list where all matched ad frames will go	


def process(p):
	# load the image from disk
	image = cv2.imread(p)
	# convert the image to grayscale and compute the hash
	image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
	imageHash = dhash(image)
	return imageHash,p


path2=[]
path=[]
#path stores the path of matched ad_frames and path2 stores the path of matched stream frames
#multiprocessing 
if __name__ == "__main__":
	def calc(fps):
		p = multiprocessing.Pool(10)
		#imap will give us the result as it gets calculated and we can insertin our database using push function after 
		#seeing if matches but if we have used map  then it would haven't given us the result as it calculates it
		#map will give the result after all values got calculated and will given us a list of result 
		for x,y in p.imap(process,res1):
			if x in hash1:
				path.append(hash1[x])
				path2.append(y)
				print("stream "+y+" matches to ")
				print(hash1[x])
				print("next match")
				s=hash1[x].split('\\') [-2]
				doc=query({"ads":s})
				push(x,y,fps,hash1,doc)
				hash1.pop(x)
		p.close()
		p.join()
	#fdur is a list that contains duration of each ad	
	fps=red("fps60")
	fdur = [int(np.ceil(x / float(fps))) for x in size]
	mongo(ads,fdur)	
	create_table()
	calc(fps)
					
finish=time.perf_counter()	
print(f'finished in {round(finish-start,2)}second(s)')


