from pymongo import MongoClient
import requests
import json

client = MongoClient()

client = MongoClient('mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb')
companies = ["DJI","AAPL","MSFT","GOOGL","FB","BRK","V","JNJ","WMT","PG","BAC","MA","T","DIS","INTC","KO","WFC","PFE","NVS","BA","TM","PEP","ORCL","CSCO","MDT","NKE","ABT","MCD","NFLX","TOT","COST","NVDA","PM","ACN","BP","TMO","IBM","ASML","SNY","HDB","SBUX","QCOM","UPS","CHTR","TD","AXP","MMM","CVS","GE","AMT","USB","DEO","BTI","LOW","UN","FIS","GILD","MS","SNE","CAT","FISV","MDLZ","BLK","SYK","ADP","EL","BDX","MUFG","ABEV","BNS","PNC","INTU","TMUS","DUK"]
for sym in companies:
	mydb = client["new6"]
	mycol = mydb["stocks"]
	if mycol.count({ 'SYM': sym }, limit = 1) != 0:
		continue
	URL = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&apikey=JMA2CT5G2880NOM7&outputsize=full&symbol="+sym
	r = requests.get(url = URL)
	data = r.json() 
	
	i = mycol.count()
	cnt=0
	stocks = []
	print(sym)
	# print(type(data["Time Series (Daily)"]))
	for key, value in data["Time Series (Daily)"].items():
		temp = {}
		temp["_id"] = i
		i+=1
		temp["SYM"] = sym
		temp["open"] = value["1. open"]
		temp["high"] = value["2. high"]
		temp["low"] = value["3. low"]
		temp["close"] = value["4. close"]
		temp["volume"] = value["5. volume"]
		chg = (float(temp["close"]) - float(temp["open"]))/float(temp["open"])
		if chg > 20:
			chg = 20
		if chg < -20:
			chg = -20

		temp['sentiment_score'] = 100*(chg/20)
		stocks.append(temp)
		stocks[cnt]["date"] = key
		cnt+=1
		# print(stocks)
	for i in range(len(stocks)):
		x = mycol.update({"date":stocks[i]["date"], "SYM":stocks[i]["SYM"]}, stocks[i], upsert=True)
		print(x)





# # print(client)

# db = client.pymongo_test

# posts = db.posts
# post_data = {
#     'title': 'Python and MongoDB',
#     'content': 'PyMongo is fun, you guys',
#     'author': 'Scott'
# }
# result = posts.insert_one(post_data)
# print('One post: {0}'.format(result.inserted_id))