#this is the link to the base of the code http://stats.seandolinar.com/collecting-twitter-data-using-a-python-stream-listener/
#importing the tweepy library and its modules for listening to Twitter streams
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

#
import requests
import time
import io
import os
import json
import threading
import multiprocessing
from datetime import datetime, timedelta

#importing the boto library for managing the data using AWS S3 buckets
import boto
import sys, os
from boto.s3.key import Key
import boto3
import _credentials
# setting up the credentials for accessing the AWS s3 buckets
LOCAL_PATH = _credentials.LOCAL_PATH
AWS_ACCESS_KEY_ID = _credentials.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = _credentials.AWS_SECRET_ACCESS_KEY

# setting up the credentials for accessing the Twitter Stream through the Streaming API
ckey = _credentials.ckey
consumer_secret = _credentials.consumer_secret
access_token_key = _credentials.access_token_key
access_token_secret = _credentials.access_token_secret

#This part of the code is taken from http://stats.seandolinar.com/collecting-twitter-data-using-a-python-stream-listener/

class listener(StreamListener):

    def __init__(self, start_time, time_limit):
        self.time = start_time
        self.limit= time_limit
        self.tweet_data = []



    def on_data(self, data):
        localtime = datetime.now().strftime("%Y-%b-%d--%H-%M-%S")
        #print(localtime)

        while (time.time() - self.time) < self.limit:
            try:
                self.tweet_data.append(data)
                return True

            except BaseException:
                print ('failed ondata')
                time.sleep(5)
                pass

        saveFile= io.StringIO()
        saveFile.write(u'[\n')
        saveFile.write(','.join(self.tweet_data))
        saveFile.write(u'\n]')
        #Here the code deviates from the original code as closed objects cannot be sent to S3 buckets

        #Here the object is sent as a json object to the S3 bucket
        #reference https://stackoverflow.com/questions/15085864/how-to-upload-a-file-to-directory-in-s3-bucket-using-boto
        s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        bucket_name2="jsonraw"
        filename=('raw_tweets_{}.json').format(localtime)
        print(filename)
        s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        s3_resource.Object(bucket_name2, filename).put(Body=saveFile.getvalue())
        print(bucket_name2, filename)
        exit()



    def on_error(self, status):

        print (status)

    def on_disconnect(self, notice):

        print ('bye')




#Here the hashtags are specified to track all relevant parties
keyword_list = ['#btw17','#Merkel','@CDU','#Schulz','@spdde','#AFD','@AfD_Bund','#Gauland', '@Alice_Weidel', '#FDP', '@c_lindner', '@fdp','#Gruene','@Die_Gruenen','@cem_oezdemir', '@GoeringEckardt', '#Linke', '@DietmarBartsch', '@SWagenknecht', '@dieLinke'] #track list

#All this initiates the classes
start_time=time.time()
auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)
#Set up to run for 14.5 minutes and then close
twitterStream = Stream(auth, listener(start_time, time_limit=870))
#Filtering the stream for the keywords and tweets in German
twitterStream.filter(track=keyword_list, languages=['de'])
