import twitter
import re 
from textblob import TextBlob
from kafka import KafkaProducer
import json
import time

# initialize api instance
twitter_api = twitter.Api(consumer_key='8VZ4lA57oPFllYYUlvgGZT1AC',
                        consumer_secret='2ti5D47N9CtEy6fdVIDqw0DuvXat7l9zxzLtbZTfquV5slTmDz',
                        access_token_key='1203866630691921920-10SjWpeJamHBSpFTNuXGdgHJKDh3h0',
                        access_token_secret='5BiI0q01XVWxZoGYfGcil6ovBZHmjrIgXNMXfoYX99GOD')


def buildTestSet(search_keyword):
    try:
        tweets_fetched = twitter_api.GetSearch(search_keyword, count = 300,since="2014-07-19")
        
        # print("Fetched " + str(len(tweets_fetched)) + " tweets for the term " + search_keyword)

        # print(tweets_fetched)
        return [{"text":status.text, "label":None,"created":status.created_at,"_id":status.id} for status in tweets_fetched]
    except Exception as e: 
    	print(e)
    	print("Unfortunately, something went wrong..")
    	return None
def clean_tweet(tweet): 
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split()) 
  
def get_tweet_sentiment(tweet):
    print(tweet)
    analysis = TextBlob(clean_tweet(tweet)) 
    return analysis.sentiment.polarity

search_term = input("Enter a search keyword:")
testDataSet = buildTestSet(search_term)


producer = KafkaProducer()
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for i in range(len(testDataSet)):
    # testDataSet[i]['sentiment'] = get_tweet_sentiment(testDataSet[i]['text'])
    time.sleep(5)
    x = producer.send('fizzbuzz', testDataSet[i])
    print(x)

