import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs





def run_twitter_etl():
    access_key = "qPnXtTw3aC8qu1MCNSadNB8Fp"
    access_secret = "xemTKsxsuJktYCgvkIYi9pQBvkb2AMrQAjvOH1rx3KqcPJgq72"
    consumer_key = "1680163377492967424-2PW20zY60Cy6HOPX57ahcdySMeKs16"
    consumer_secret = "JnEoiGUu0rktfgbVmVPLFnfgLboz6LW79c8nYyWybHcQe"

    # Twitter authentication to make a connection between our code and twitter API to retreive twitter data
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                               # 200 is the maximum allowed count that will be extracted
                               count=200,
                               #iclude or not reposted posts from elon mask
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

#transform the json data to a csv file to be more clear
     list = []
     for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://mybucketname/elon_mask_refined_tweets.csv')
