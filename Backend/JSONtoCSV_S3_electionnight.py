import boto
import sys, os
from boto.s3.key import Key
import boto3
import csv, glob, re, io, json, time, os
#Imports the credentials to access  the amazon s3 buckets
#for storing the tweets
import _credentials


LOCAL_PATH = _credentials.LOCAL_PATH
AWS_ACCESS_KEY_ID = _credentials.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = _credentials.AWS_SECRET_ACCESS_KEY

#Name of the S3 bucket with all tweets in JSON form
bucket_name = 'jsonraw'

#connecting to the bucket
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)

from datetime import datetime, timedelta
#normally this script wants to access to the newest 24 hours
localtime = datetime.now().strftime("%Y-%b-%d--%H-%M-%S")
#but as we want to replicate the results of the election night we set the time
#to the closure of the polls (18-00-00), as the files are saved with UTC-time and the
#difference to German Summer Time is 2 hours, we set it to (16-00-00)
localtime="2017-Sep-24--16-00-00"
print(localtime)
#converting the format of the time to fit to the names of the JSON files
localtime=(datetime.strptime(localtime, '%Y-%b-%d--%H-%M-%S'))

#we count the number of files processed and summed up to the two
#CSV-files, one for the last 24hours and the other one for the time window
#of 24-48hours ago
filecount=0
filecount2=0
day=(str(localtime))

#these are the io.Strings for these two strings
csv_out1 = io.StringIO()
csv_out2 = io.StringIO()



s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket = s3.Bucket('jsonraw')
# Iterates through all the objects in the bucket
for obj in bucket.objects.all():
    #retrieves the key which is the filename
    key = obj.key
    print(key)

    filenameS3=key
    #does a regex, a group function and takes the string without ".json"
    #to retrieve the time when the file was created and change it into a datetime format
    filename= re.match(r"^.*tweets_(.*)\.*$",key)
    filename=(filename.group(1))
    timestring=(filename[:-5])
    filetime=(datetime.strptime(timestring, '%Y-%b-%d--%H-%M-%S'))
    #calculates the time delta, so the time passed since the localtime and the creation of
    #this json-file
    delta=(localtime-filetime)
    print(delta)

    #if the time difference is less than 24 hours but also not negative, the file is used
    if timedelta(days=0)<delta < timedelta(days=1):

        print("new")
        filecount+=1
        #reads in the JSON file
        try:
            s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            data_python = json.loads(obj['Body'].read().decode('utf-8'))

        except ValueError:
            data_python=""
        print("file number: ", filecount)
        #should not be necessary but breaks the collection of the JSON objects in case it finds more than expected
        #98=24hours*4files for 15 minute window
        if filecount>96:
                break
        #creates the top row of the later csv
        fields = u'created_at,text,rt_text,quoted_text,tweet_id,screen_name,followers,user_id,friends,rt,fav' #field names
        csv_out1.write(fields)
        csv_out1.write(u'\n')
        #now iterates through each line of the loaded json file
        for line in data_python:


                rt_text=""
                quoted_text=""
                #if a tweet quotes another tweet, it retrieves this quoted tweet's text
                if line.get('quoted_status'):
                        #print("Quote")
                        if line.get('quoted_status').get('extended_tweet'):
                            quoted_text=line['quoted_status']['extended_tweet']['full_text']
                            quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')
                #if a tweet retweets another tweet, it retrieves this  retweeted tweet's text
                if line.get('retweeted_status'):
                        #print("Retweet")
                        rt_text=line['retweeted_status']['text']
                        rt_text=('"' + rt_text.replace('"','').replace("'","") + '"')
                        if line.get('retweeted_status').get('quoted_status'):
                            quoted_text=line['retweeted_status']['quoted_status']['text']
                            quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')
                #if a tweet has its own text and is not just a simple text, it retrieves this tweet's text
                if line.get('text') != None:
                        #print("Text")
                        text=line.get('text')
                        text=('"' + text.replace('"','').replace("'","") + '"')
                #fills up the row with the different elements of the json object such as user-id etc
                        row = [line.get('created_at'),
                                   text,
                                   rt_text,
                                   quoted_text,#creates double quotes
                                   line.get('id_str'),
                                   line.get('user').get('screen_name'),
                                   (line.get('user').get('followers_count')),
                                   (line.get('user').get('id_str')),
                                   (line.get('user').get('friends_count')),
                                   (line.get('retweet_count')),
                                   (line.get('favorite_count'))]
                else:
                    row=""
                #removes all brackets from the rows
                row_joined = (str(row).strip('[').strip(']'))
                #writes them to a csv
                csv_out1.write(row_joined)
                csv_out1.write(u'\n')

    #this is the exact same structure except it takes all of the tweets that
    #were posted a day ago before the polls closed, but that are also not older than 48 hours
    #this creates the file to train the machine learning model
    if timedelta(days=1) < delta < timedelta(days=2):
        print("model")


        filecount2+=1

        try:
            print(bucket_name," ", key)

            s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


            obj = s3.get_object(Bucket=bucket_name, Key=key)
            data_python = json.loads(obj['Body'].read().decode('utf-8'))


        except ValueError:
            data_python=""
        print("file number: ", filecount2)

        if filecount2>98:
                break

        fields = u'created_at,text,rt_text,quoted_text,tweet_id,screen_name,followers,user_id,friends,rt,fav' #field names
        csv_out2.write(fields)
        csv_out2.write(u'\n')
        for line in data_python:

                rt_text=""
                quoted_text=""

                if line.get('quoted_status'):
                        #print("Quote")
                        if line.get('quoted_status').get('extended_tweet'):
                            quoted_text=line['quoted_status']['extended_tweet']['full_text']
                            quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')

                if line.get('retweeted_status'):
                        rt_text=line['retweeted_status']['text']
                        rt_text=('"' + rt_text.replace('"','').replace("'","") + '"')
                        if line.get('retweeted_status').get('quoted_status'):
                            quoted_text=line['retweeted_status']['quoted_status']['text']
                            quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')

                if line.get('text') != None:
                        text=line.get('text')
                        text=('"' + text.replace('"','').replace("'","") + '"')

                        row = [line.get('created_at'),
                                   text,
                                   rt_text,
                                   quoted_text,
                                   line.get('id_str'),
                                   line.get('user').get('screen_name'),
                                   (line.get('user').get('followers_count')),
                                   (line.get('user').get('id_str')),
                                   (line.get('user').get('friends_count')),
                                   (line.get('retweet_count')),
                                   (line.get('favorite_count'))]
                else:
                    row=""

                row_joined = (str(row).strip('[').strip(']'))
                csv_out2.write(row_joined)
                csv_out2.write(u'\n')
    if timedelta(days=2) < delta:
        print("Old!")
    if timedelta(days=0) > delta:
        print("Future!")

#stores the two created csv files with all tweets from either
#last 24hours or 24 to 48hours
bucket_name2="jsontocsv2"
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object(bucket_name2, 'Last24.csv').put(Body=csv_out1.getvalue())
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object(bucket_name2, '24to48.csv').put(Body=csv_out2.getvalue())
