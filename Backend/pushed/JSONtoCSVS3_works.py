


def lambda_handler(one, two):
    import boto
    import sys, os
    from boto.s3.key import Key
    #from io import io.StringIO
    import boto3
    import _credentials
    import csv, glob, re, io, json, time, os
    from datetime import datetime, timedelta


    LOCAL_PATH = _credentials.LOCAL_PATH
    AWS_ACCESS_KEY_ID = _credentials.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = _credentials.AWS_SECRET_ACCESS_KEY

    ckey = _credentials.ckey
    consumer_secret = _credentials.consumer_secret
    access_token_key = _credentials.access_token_key
    access_token_secret = _credentials.access_token_secret
    bucket_name = 'jsonraw'


    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY)

    from datetime import datetime, timedelta
    localtime = datetime.now().strftime("%Y-%b-%d--%H-%M-%S")
    print(localtime)
    localtime=(datetime.strptime(localtime, '%Y-%b-%d--%H-%M-%S'))

    filecount=0
    filecount2=0
    day=(str(localtime))

    csv_out1 = io.StringIO()
    csv_out2 = io.StringIO()

    #df.to_csv(csv_buffer)
    #print(str(localtime))
    #csv_out1 = io.open("Last24.csv", mode='w', encoding='utf-8') #opens csv file
    #csv_out1 = io.open("24to48.csv", mode='w', encoding='utf-8') #opens csv file


    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    bucket = s3.Bucket('jsonraw')
    # Iterates through all the objects, doing the pagination for you. Each obj
    # is an ObjectSummary, so it doesn't contain the body. You'll need to call
    # get to get the whole body.
    for obj in bucket.objects.all():
        key = obj.key
        print(key)
        #file = obj.get()['Body'].read()
        #print(file)
        filenameS3=key
        filename= re.match(r"^.*tweets_(.*)\.*$",key)
        filename=(filename.group(1))
        timestring=(filename[:-5])
        #print(timestring)
        filetime=(datetime.strptime(timestring, '%Y-%b-%d--%H-%M-%S'))
        delta=(localtime-filetime)
        print(delta)

        if delta < timedelta(days=1):
            #data_python=file

            print("new")
            #data_json = io.open(file, mode='r', encoding='utf-8').read()
            filecount+=1
            #print(file)
            #reads in the JSON file
            try:
                s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                print(bucket_name," ", key)
                s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

                #s3 = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

                obj = s3.get_object(Bucket=bucket_name, Key=key)
                data_python = json.loads(obj['Body'].read().decode('utf-8'))

            except ValueError:
                data_python=""
            print("file number: ", filecount)

            if filecount>98:
                    break

            fields = u'created_at,text,rt_text,quoted_text,tweet_id,screen_name,followers,user_id,friends,rt,fav' #field names
            csv_out1.write(fields)
            csv_out1.write(u'\n')
            for line in data_python:

                    #print(line)
                    #writes a row and gets the fields from the json object
                    #screen_name and followers/friends are found on the second level hence two get methods

                    rt_text=""
                    quoted_text=""

                    if line.get('quoted_status'):
                            #print("Quote")
                            if line.get('quoted_status').get('extended_tweet'):
                                quoted_text=line['quoted_status']['extended_tweet']['full_text']
                                quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')

                    if line.get('retweeted_status'):
                            #print("Retweet")
                            rt_text=line['retweeted_status']['text']
                            rt_text=('"' + rt_text.replace('"','').replace("'","") + '"')
                            if line.get('retweeted_status').get('quoted_status'):
                                quoted_text=line['retweeted_status']['quoted_status']['text']
                                quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')

                    if line.get('text') != None:
                            #print("Text")
                            text=line.get('text')
                            text=('"' + text.replace('"','').replace("'","") + '"')

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

                    row_joined = (str(row).strip('[').strip(']'))
                    csv_out1.write(row_joined)
                    csv_out1.write(u'\n')

        if timedelta(days=1) < delta < timedelta(days=2):
            print("model")
            #data_python=file

            #data_json = io.open(filenameS3, mode='r', encoding='utf-8').read()
            filecount2+=1
            #print(file)
            #reads in the JSON file
            try:
                print(bucket_name," ", key)

                s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

                #s3 = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

                obj = s3.get_object(Bucket=bucket_name, Key=key)
                data_python = json.loads(obj['Body'].read().decode('utf-8'))
                #data_python = json.loads(obj.get()['Body'].read().decode('utf-8') )


                #data_python = json.loads(obj['Body'].read())
            except ValueError:
                data_python=""
            print("file number: ", filecount)

            if filecount2>98:
                    break

            fields = u'created_at,text,rt_text,quoted_text,tweet_id,screen_name,followers,user_id,friends,rt,fav' #field names
            csv_out2.write(fields)
            csv_out2.write(u'\n')
            for line in data_python:

                    #print(line)
                    #writes a row and gets the fields from the json object
                    #screen_name and followers/friends are found on the second level hence two get methods

                    rt_text=""
                    quoted_text=""

                    if line.get('quoted_status'):
                            #print("Quote")
                            if line.get('quoted_status').get('extended_tweet'):
                                quoted_text=line['quoted_status']['extended_tweet']['full_text']
                                quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')

                    if line.get('retweeted_status'):
                            #print("Retweet")
                            rt_text=line['retweeted_status']['text']
                            rt_text=('"' + rt_text.replace('"','').replace("'","") + '"')
                            if line.get('retweeted_status').get('quoted_status'):
                                quoted_text=line['retweeted_status']['quoted_status']['text']
                                quoted_text=('"' + quoted_text.replace('"','').replace("'","") + '"')

                    if line.get('text') != None:
                            #print("Text")
                            text=line.get('text')
                            text=('"' + text.replace('"','').replace("'","") + '"')

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

                    row_joined = (str(row).strip('[').strip(']'))
                    csv_out2.write(row_joined)
                    csv_out2.write(u'\n')
        if timedelta(days=2) < delta:
            print("Old!")
    bucket_name1="jsontocsv"

    bucket_name2="jsontocsv2"
    s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_resource.Object(bucket_name2, 'Last24.csv').put(Body=csv_out1.getvalue())
    s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_resource.Object(bucket_name1, '24to48.csv').put(Body=csv_out2.getvalue())
