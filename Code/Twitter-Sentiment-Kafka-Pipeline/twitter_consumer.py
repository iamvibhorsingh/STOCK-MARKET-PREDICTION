import twitter
import re 
from textblob import TextBlob
from kafka import KafkaConsumer
import json
from pymongo import MongoClient
import time

client = MongoClient()
client = MongoClient('mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb')

mydb = client["sto"]
mycol = mydb["new_tweets"]

def clean_tweet(tweet): 
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split())
    
def get_tweet_sentiment(tweet):
    print(tweet)
    analysis = TextBlob(clean_tweet(tweet)) 
    return analysis.sentiment.polarity

consumer = KafkaConsumer('fizzbuzz',
                         group_id=None,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', 
                         enable_auto_commit=True,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    m = message.value
    m['sentiment'] = get_tweet_sentiment(m['text'])
    x = mycol.update({"_id":m["_id"]}, m, upsert=True)
    time.sleep(5)
    print("sentiment score",m['sentiment'])
    print(x) 
