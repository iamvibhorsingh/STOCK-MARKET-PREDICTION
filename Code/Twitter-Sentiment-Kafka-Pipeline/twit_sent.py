import re 
from textblob import TextBlob 

from pymongo import MongoClient 
  
def clean_tweet(tweet): 
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split()) 
  
def get_tweet_sentiment(tweet):
    print(tweet)
    analysis = TextBlob(clean_tweet(tweet)) 
    return analysis.sentiment.polarity

client = MongoClient()
client = MongoClient('mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb')
mydb = client["sto"]
mycol = mydb["tweets"]
data = mycol.find_one()
print(get_tweet_sentiment(data["text"]))