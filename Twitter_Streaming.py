import sys

from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
from tweepy.streaming import StreamListener
import pandas as pd
from textblob import TextBlob
import re
from geopy.geocoders import Nominatim
from datetime import datetime

# Replace the "None"s by your own credentials
ACCESS_TOKEN = None
ACCESS_TOKEN_SECRET = None
CONSUMER_KEY = None
CONSUMER_SECRET = None


auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)
num_tweets = 0

class Listener(StreamListener):#Extends the StreamListener class from tweepy.streaming to override on_status and on_error methods
    def __init__(self, dataframe):
        super(Listener,self).__init__()
        self.tweet_data = dataframe
    def on_status(self, status):
        global num_tweets
        global stream
        
        if status.retweeted:#Ignore tweets that have been retweeted
            return
        if status.text[0:3] == "RT ":#Ensure no retweets, since some will get through
            return
        #status.text
        orig_tweet = ""
        try:
            orig_tweet = status.extended_tweet['full_text']
        except AttributeError as e:
            orig_tweet = status.text
        cleaned_text = self.cleanText(orig_tweet)
        sentiment = TextBlob(cleaned_text).sentiment.polarity
        code = self.geo_locator(status.user.location)
        if code == None:
            code = "None"
        new_row = {'Date(YYYY-MM-DD hh:mm:ss)':status.created_at, 'Username':status.user.screen_name, 'Country Code':code, 'Tweet Id':status.id,
                    'Original Tweet':orig_tweet, 'Cleaned Tweet':cleaned_text, 'Tweet URL': "https://twitter.com/"+str(status.user.screen_name)+"/status/"+str(status.id),
                    'Sentiment Score(-1 <= x <= 1)':sentiment}
        #append row to the dataframe
        self.tweet_data = self.tweet_data.append(new_row, ignore_index=True)
        num_tweets += 1
        if  (num_tweets == 100):
            stream.disconnect()
    def on_error(self, status_code):
        print(status_code)
        return False

    def cleanText(self, text):
        #Tweets tend to have special characters coming from links, emojis, #hashtags and @'s. This function cleans the text
        #for proper sentiment analysis of the text 
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split())

    def returnDataFrame(self):
        return self.tweet_data
    
    def geo_locator(self, user_location):
        #Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright. From the license
        #Locate the country that the tweet originated from
        #Initialize geolocator
        geolocator = Nominatim(user_agent='Tweet_locator')
        if user_location is not None:
            try :
                #get location
                location = geolocator.geocode(user_location, language='en')
                location_exact = geolocator.reverse(
                    [location.latitude, location.longitude], language='en')
                #get country
                country = location_exact.raw['address']['country_code']
                return country
            except:
                return None

        else : 
            return None
        

#output = open('stream_output.txt', 'w', encoding='utf-8')
tweet_data = pd.DataFrame(columns=["Date(YYYY-MM-DD hh:mm:ss)", "Username", "Country Code", 
                "Tweet Id", "Original Tweet", "Cleaned Tweet", "Tweet URL", "Sentiment Score(-1 <= x <= 1)"])
listener = Listener(dataframe=tweet_data)
stream = Stream(auth=api.auth, listener=listener)

try:
    my_date = datetime.now()
    
    print('Start streaming: ' + my_date.strftime('%Y-%m-%d %H:%M:%S'))
    stream.filter(track=["Trump"], languages=['en'])
    tweet_data = listener.returnDataFrame()
    my_date = datetime.now()
    print("Collected {:} Tweets. Transferring to CSV file: ".format(num_tweets) + my_date.strftime('%Y-%m-%d %H:%M:%S'))
    tweet_data.to_csv(r'C:\Users\gannuv\Documents\GitHub\Twitter-Sentiment-Analysis\stream_output.csv', index = False, header=True)
    print('CSV file ready')
    
    #Run this section another day. Reads CSV file and adds more tweets
    # tweet_data = pd.read_csv('stream_output.csv')
    # listener = Listener(dataframe=tweet_data)
    # stream = Stream(auth=api.auth, listener=listener)
    # print('Start streaming 2: ' + my_date.strftime('%Y-%m-%d %H:%M:%S'))
    # stream.filter(track=["Trump"], languages=['en'])
    # tweet_data = listener.returnDataFrame()
    # my_date = datetime.now()
    # print("Collected {:} Tweets. Transferring to CSV file: ".format(num_tweets) + my_date.strftime('%Y-%m-%d %H:%M:%S'))
    # tweet_data.to_csv(r'C:\Users\gannuv\Documents\GitHub\Twitter-Sentiment-Analysis\stream_output.csv', index = False, header=True)
    # print('CSV file ready')

except KeyboardInterrupt:
    print("Stopped. Stream disconnecting")
    stream.disconnect()