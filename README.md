## STOCK MARKET PREDICTION

## Problem Statement

```
● We primarily attempt to create a data platform that predicts the price of a
scrip/index for an interval of time. We use historical quotes to anticipate market
movements, thereby enabling intelligent decision making for portfolio managers.


● Subsequently, we factor in how twitter sentiment about the market and the
financial sector can play a huge role in influencing trades


● We’ve provided API as a service over trained machine learning model that
facilitates prediction based on some parameters.
```


## Data Source

```
● We consumed data from the alphavantage API -
https://www.alphavantage.co/query?function=TIME_SERIES_DAILY


● We also pull tweets from the public Twitter Search API -
https://api.twitter.com/1.1/search/tweets.json
```

## Big Data Pipeline
![Pipeline](https://github.com/iamvibhorsingh/STOCK-MARKET-PREDICTION/blob/master/Code/Jupyter-Notebook/big%20data%20pipeline.png)

## Architecture
![Project Architecture](https://github.com/iamvibhorsingh/STOCK-MARKET-PREDICTION/blob/master/Code/Jupyter-Notebook/Architecture.png)

## Tools

```
● Alphavantage API
● Twitter Public Search API
● Kafka
● TextBlob
● MongoDB
● AWS S
● PySpark
● Spark ML
● Flask based REST API
● Tableau
```

## Features

```
● Open price
● Closing price
● Low price
● High price
● Volume
● Date
● Sentiment Score
● SYM (DJI, MSFT, etc)
```

## Pre-processing

```
● Removed all NaN values


● Estimated sentiment values for historical quotes


● Transformations to remove skewness


● Vector Scaling


● Removal of outliers
```

### Sentiment Analysis via Kafka


## Model Results


## Model Accuracy - RandomForestRegressor

```
● Predicted market close price for intervals (day) in range of 2% change for
~70% of the test data.
● Mean average error = 171.
● R2 = 0.
```

## API Service


## Conclusion

```
● Financial markets are highly erratic and trends cannot be prediction with a very
high accuracy for a long period in time. For a short interval of time, they can
provide better results and assist with HFT.


● In this project, we’ve designed a prototype architecture that can evolve into a HFT
system.


● The fundamental idea is feasible and with use of deep learning models and larger
data size, we will be able grow a market intelligence data platform.
```

## Future scope

```
● Our architecture uses tools that are horizontally scalable by design. In the future, if
we switch from interval of 1 day to say 1 minute, the architecture will be able to
scale by adding only compute instance.


● Deep learning models may also prove much better in improving accuracy of results.


● Enhanced real time pipeline and using efficient third party service for NLP can allow
better sentiment scores for tweets. This will aid in refined prediction results.


● Our’s is a loosely coupled architecture i.e. each service within the pipeline is plug &
play. This means upgrading the architecture with future services is simpler.


● A user interface can be added that allows portfolio management team to better
visualize results.
```



