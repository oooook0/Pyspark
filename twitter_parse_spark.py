import nltk
import ujson
import fnmatch
from bz2 import BZ2File as bzopen
import os
import time
start_time = time.time()

path = "/Users/sunyitao/Desktop/00"
outputFile="/Users/sunyitao/Desktop/01"

data = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(path)
        for f in fnmatch.filter(files, '*.bz2')]

def parse_tweet(t):
        row = {}
        try:
                lang = t['lang']
        except:
                lang = t['user']['lang']
        text = t['text'].replace('\n', ' ').replace(",", ' ').encode('utf-8')
        try:
                hasht = t['entities']['hashtags'][0]['text']
        except:
                hasht = ''
        try:
                followers = t['user']['followers_count']
                utc = t['user']['utc_offset']
        except:
                followers = None
                utc = ''
        try:
                retw = t['retweeted']
        except:
                retw = False
        row = {'id_str': t['id_str'], 'created_at': t['created_at'], 'utc_offset': utc,
                'favorited': t['favorited'], 'retweeted': retw, 'retweet_count': t['retweet_count'],
                'hashtags': hasht, 'lang': lang, 'followers_count': followers,  'text': text}
        return(row)

input=sc.union([sc.textFile(f) for f in data])

datafile = input.map(lambda x: ujson.loads(x))

datafilter = datafile.filter(lambda x: len(x) > 1).map(lambda x: parse_tweet(x))

datafilter.count()

datafilter.map(lambda x: ujson.dumps(x)).saveAsTextFile(outputFile)

print("--- %s seconds ---" % (time.time() - start_time))
