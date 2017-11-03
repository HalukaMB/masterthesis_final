#As not a mere text is saved but actually also a machine learning algorithm model
#joblib is needed to save this model

import joblib
import s3io
import boto3
import _credentials
from io import StringIO


# setting up the credentials for accessing the AWS s3 buckets
LOCAL_PATH = _credentials.LOCAL_PATH
AWS_ACCESS_KEY_ID = _credentials.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = _credentials.AWS_SECRET_ACCESS_KEY
bucket_name = 'jsontocsv2'

import csv, glob, re, io, json, time, os, boto
import pandas as pd

#for some reason boto3 alone would not help in storing machine learning models
#therefore, also boto is used
#again, to prevent the replication from interfering with the live application
# a different bucket is used with "jsontocsv2" instead of "jsontocsv"
#the code for accessing s3 stems from https://stackoverflow.com/questions/15085864/how-to-upload-a-file-to-directory-in-s3-bucket-using-boto
#and https://stackoverflow.com/questions/30818341/how-to-read-a-csv-file-from-an-s3-bucket-using-pandas-in-python
from boto.s3.key import Key
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
bucket = s3.Bucket('jsontocsv2')
s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

#reads in the csv with all tweets from the last day (48h to 24h)
from boto.s3.key import Key
k = Key('jsontocsv2')
k.key = '24to48.csv'
data = pd.read_csv('https://s3.amazonaws.com/jsontocsv2/24to48.csv', sep=',' , skipinitialspace = True, quotechar = "'")

print(data.head())
#sorting the csv for the user_id
data=data.sort_values(by='user_id')

#We are creating two lists. One with ids of likely bot accounts and one with ids of "normal-accounts"

#this loop iterates through the csv and records which account-ids have tweeted more than 24 times
#and appends them to a list. Our bot-list
IDbot=0
counter=0
botlist=[]
for cell in data["user_id"]:
    #if the counter is over 24, the id of the account is added to the botlist,
    #unless the id is already in there
    if counter>24:
        if IDbot not in botlist and IDbot != "user_id":
            botlist.append(IDbot)
        else:
            pass
    #if the current cell has a different id than the stored ID,
    #the stored ID is changed to the cell-ID and counter resetted to 1
    if cell != IDbot:
        IDbot=cell
        counter=1
    #otherwise the ID stays the same but the counter is increased by 1
    else:
        counter+=1

#this loop iterates through the csv and records which account-ids have tweeted only once
#and appends them to a list and is our "normal-list"
IDnor=0
#counter has to be set at the beginning to zero to avoid the first ID getting
#appended rightaway to the list of normal accounts
counter=0
normallist=[]
for cell in data["user_id"]:
#if the current cell is not the same as the previously stored IDnor
#and the counter is at 1, the ID is appended to the list of normal accounts
    if cell!=IDnor:
        if counter==1:
            normallist.append(IDnor)
            counter=0
        else:
            counter=0
    else:
        pass
    #this increases the counter
    counter+=1
    IDnor=cell
if (counter==1):
    normallist.append(IDnor)



#reduces the csv to only text, the id of the account and the id of the tweet
reduced_data=pd.concat([data["text"], data["user_id"], data["tweet_id"]], axis=1)


botsampleid = []
normalsampleid = []
botsampletext=[]
normalsampletext=[]
bottweetid = []
normaltweetid = []

progcounter=0
for index, row in reduced_data.iterrows():
    progcounter+=1
    #if the id is in our created list of bot accounts,
    #the id, the text and the tweets id are stored in
    #three separate lists
    if(row['user_id'])in botlist:
        botsampleid.append(row['user_id'])
        botsampletext.append(row['text'])
        bottweetid.append(row['tweet_id'])

    #if the id is in our created list of "normal" accounts
    #the id, the text and the tweets id are stored in
    #three separate lists
    if(row['user_id'])in normallist:
        normalsampleid.append(row['user_id'])
        normalsampletext.append(row['text'])
        normaltweetid.append(row['tweet_id'])



    if (progcounter%1000==0):
        print(progcounter)


#we build a "bot" dataframe from these three lists and give it a fourth column
#with a default value of 1 for being a bot
botdf=pd.DataFrame()
botdf["user_id"]=botsampleid
botdf["text"]=botsampletext
botdf["tweet_id"]=  bottweetid
botdf["botornot"]=1

#we build a "normal" dataframe from these three lists and give it a fourth column
#with a default value of 1 for being a bot
normaldf=pd.DataFrame()
normaldf["user_id"]=normalsampleid
normaldf["text"]=normalsampletext
normaldf["tweet_id"]= normaltweetid
normaldf["botornot"]=0

#getting rid of the index column
normaldf.reset_index(drop=True, inplace=True)
botdf.reset_index(drop=True, inplace=True)

#removing all rows where the text is just 'text'
normaldf = normaldf[normaldf.text != 'text']
botdf = botdf[botdf.text != 'text']

#assembling from the collected dataframes two samples of size 1000
normaldf=(normaldf.sample(1000, replace=True))
botdf=(botdf.sample(1000, replace=True))

csv_buffer = StringIO()
#saving the tweets classified as normal to the s3 bucket

normaldf.to_csv(csv_buffer, quotechar='"')
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'normaltweets_yesterday.csv').put(Body=csv_buffer.getvalue())

csv_buffer = StringIO()
#saving the tweets classified as bot to the s3 bucket
botdf.to_csv(csv_buffer, quotechar='"')
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'bottweets_yesterday.csv').put(Body=csv_buffer.getvalue())


#creating a training dataset for the algorithm by concating
#the bot-dataframe and the normal-dataframe
test=pd.concat([normaldf,botdf], ignore_index=True)

print(test.head())
print(test.tail())
#by using sample frac=1, we get the whole dataframe but randomly shuffled
test = test.sample(frac=1).reset_index(drop=True)
print(test.head())
print(test.tail())

#now we load in the data to the script that creates a stylometric fingerprint
#for each tweet
data = test

#the principle is as follows, first we create a list for each aspect that the script catches
# and then we puzzle together an array/dataframe from these lists
dotcountlist=[]
commacountlist=[]
exclamationcountlist=[]
wordcountlist=[]
uppercasecountlist=[]
lowercasecountlist=[]
charcountlist=[]
specialcharlist=[]
numbercountlist=[]
uniquewordlist=[]
qmarkcountlist=[]
apicescountlist=[]
quotescountlist=[]
openparlist=[]
closeparlist=[]
operatorlist=[]
hashtagcountlist=[]
dottycountlist=[]
linkcountlist=[]




a=0
#for each cell in the text column of data
for cell in (data['text']):

    a+=1
    dotcount=0
    commacount=0
    qmarkcount=0
    exclamationcount=0
    wordcount=0
    uppercasecount=0
    lowercasecount=0
    charcount=0
    specialchar=0
    numbercount=0
    cellwordlist=[]
    uniqueword=0
    qmarkcount=0
    apicescount=0
    quotescount=0
    openparcount=0
    closeparcount=0
    operatorcount=0
    hashtagcount=0
    dottycount=0
    linkcount=0

    #we now count how often certain characters appear in each text cell
    dotcount=cell.count(".")
    commacount=cell.count(",")
    exclamationcount=cell.count("!")
    qmarkcount=cell.count("?")
    apicescount=cell.count("'")
    quotescount=cell.count('"')
    openparcount=cell.count('(')
    closeparcount=cell.count(')')
    hashtagcount=cell.count('#')


    #now we check for each word in the cell
    for word in (cell.split()):

        #how many unique words we find
        if word in cellwordlist:
            continue
        else:
            cellwordlist.append(word)
            uniqueword+=1
        #how many "..." do we find
        if re.search('\.\.\.',word):
            dottycount+=1
        #how many links can we find
        if re.search('http',word):
            linkcount+=1

        #how many words are in each cell
        wordcount+=1

        #checks how many letters, that are not at the start of a word,
        #are written in uppercase like "WHAT ARE THEY DOING???"
        uppercaseword=word[1:]
        for c in uppercaseword:
            if c.istitle():
                uppercasecount+=1

        #checks how many characters and numbers are in each word
        for char in word:
            charcount+=1
            #checks how many characters are used
            if re.match("[^a-zA-Z0-9]", char):
                specialchar+=1
            #checks how many numbers are used
            if re.match("[0-9]", char):
                numbercount+=1
            #checks how many characters are lowercase
            if char.islower():
                lowercasecount+=1
            #checks how many characters are operators
            if re.match("[\+\/\-\*\%\=<>&]", char):
                operatorcount+=1

    #appending the results of the various counters to the list
    dotcountlist.append(dotcount)
    commacountlist.append(commacount)
    exclamationcountlist.append(exclamationcount)
    wordcountlist.append(wordcount)
    uppercasecountlist.append(uppercasecount)
    lowercasecountlist.append(lowercasecount)
    charcountlist.append(charcount)
    specialcharlist.append(specialchar)
    numbercountlist.append(numbercount)
    uniquewordlist.append(uniqueword)
    qmarkcountlist.append(qmarkcount)
    apicescountlist.append(apicescount)
    quotescountlist.append(quotescount)
    openparlist.append(openparcount)
    closeparlist.append(closeparcount)
    operatorlist.append(operatorcount)
    hashtagcountlist.append(hashtagcount)
    dottycountlist.append(dottycount)
    linkcountlist.append(linkcount)





#now building pandas series from these lists
row1=pd.DataFrame()
row1["Dotcount"]=dotcountlist
row2=pd.DataFrame()
row2["Commacount"]=commacountlist
row3=pd.DataFrame()
row3["Exclcount"]=exclamationcountlist
row4=pd.DataFrame()
row4["Wordcount"]=wordcountlist
row5=pd.DataFrame()
row5["Uppercasecount"]=uppercasecountlist
row6=pd.DataFrame()
row6["Lowercasecount"]=lowercasecountlist
row7=pd.DataFrame()
row7["Specialchar"]=specialcharlist
row8=pd.DataFrame()
row8["Numbercount"]=numbercountlist
row9=pd.DataFrame()
row9["Charcount"]=charcountlist
row10=pd.DataFrame()
row10["Uniquewords"]=uniquewordlist
row11=pd.DataFrame()
row11["Qmarkcount"]=qmarkcountlist
row11=pd.DataFrame()
row11["Apicescount"]=apicescountlist
row12=pd.DataFrame()
row12["Quotescount"]=quotescountlist
row13=pd.DataFrame()
row13["Openpar"]=openparlist
row14=pd.DataFrame()
row14["Closepar"]=closeparlist
row15=pd.DataFrame()
row15["Operatorcount"]=operatorlist
row16=pd.DataFrame()
row16["Hashtagcount"]=hashtagcountlist
row17=pd.DataFrame()
row17["Dottycount"]=dottycountlist
row18=pd.DataFrame()
row18["Linkcount"]=linkcountlist

#buidling dataframes from these serieses
output=pd.concat([data, row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11, row12, row13, row14, row15, row16, row17, row18],axis=1)

#storing this stylometric fingerprint csv to the S3 bucket
csv_buffer = StringIO()
output.to_csv(csv_buffer, quotechar='"', index=False)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'botABScounts.csv').put(Body=csv_buffer.getvalue())

#only taking the user_id, the tweet_id and the botornot column without further processing
new_datareduced=output.ix[:,[0,2,3]]
#taking the dataframe from the third column onwards as the datareduced dataframe
datareduced=output.ix[:,3:]
print(datareduced.head(10))
datareduced.describe()
c=0
limit=0
#counting the number of columns of this
for column in datareduced:
    limit+=1
print(limit)
a=-1
#now we iterate through each column and calculate the average for each column
#divided by the mean number of characters
for column in datareduced:
    lista=[]
    a+=1
    columnmean=((datareduced.ix[:, a]).mean())/datareduced['Charcount'].mean()
    print(column,":" ,columnmean)
    b=-1
    #now we weight each absolute value we calculated earlier against the mean
    #so for instance, if 2 dots were counted, but the mean number of dots was 1.5
    # the relative weighted value is then 2/1.5=2.33
    for index, row in (datareduced).iterrows():
        b+=1
        columnval=(datareduced.ix[b, a])
        columnratio=columnval/datareduced.ix[b,'Charcount']
        columnREL=(columnratio/columnmean)
        lista.append(columnREL)
    c+=1
    print(c)
    if c>limit:
        print("Limit!")
        break
    else:
        column = pd.DataFrame({(column+"REL") : lista})
        #putting together the new weighted columns with the not processed values
        new_datareduced=pd.concat([new_datareduced, column], axis=1)
print("STOP!")


#dropping two columns that created problems when using for the machine learning model
new_datareduced=new_datareduced.drop("ApicescountREL", axis=1)
new_datareduced=new_datareduced.drop("botornotREL", axis=1)

#now storing again the dataframe with the now weighted relative counts
csv_buffer = StringIO()
new_datareduced.to_csv(csv_buffer, quotechar='"', encoding='utf-8', index=False)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'botRELcounts.csv').put(Body=csv_buffer.getvalue())


#creating a csv that has all the bot-ids in it
print(botlist)
botdf=pd.DataFrame()
botdf["bot_ids"]=botlist
#and storing it in S3
csv_buffer = StringIO()
botdf.to_csv(csv_buffer, quotechar='"', encoding='utf-8', index=False)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_resource.Object("sampletweets2", 'lastbotlist.csv').put(Body=csv_buffer.getvalue())

#just checking if the dataframe is looking okay
new_datareduced.head()

#loading libraries for using machine learning library scikit-learn
import numpy as np
import pandas as pd
import glob
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.metrics import confusion_matrix
import pickle
from datetime import datetime
from sklearn.cross_validation import StratifiedKFold
from sklearn import metrics
y_test=[]
pred_test=[]
#this is the function for cross-validation
#source https://github.com/scikit-learn/scikit-learn/issues/1696
def kfold(clr,X,y,folds=10):

    localtime = datetime.now().strftime("%Y-%b-%d--%H-%M-%S")
    print(localtime)

    mae_sum=0
    acc_sum=0
    #creates the cross-validation datasets to be used
    kf = StratifiedKFold(y, folds)
    a=0
    for train_index, test_index in kf:
        a+=1
        #loads from kf all data that is used as indepedent variables
        X_train, X_test = X[train_index], X[test_index]
        #loads from kf the column of the dependent variables
        y_train, y_test = y[train_index], y[test_index]
        clr.fit(X_train, y_train)
        pred_test = clr.predict(X_test)


        #saves the model after the 9th run in an s3 bucket by using .sav format and compressing gzip
        if a>9:
            bucket = "sampletweets2"
            key = "trained_model.sav"
            compress = ('gzip', 3)
            credentials = dict(aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

            # Dump in an S3 file is easy with Joblib
            with s3io.open('s3://{0}/{1}'.format(bucket, key), mode='w',**credentials) as s3_file:
                joblib.dump(clr, s3_file, compress=compress)



        #printing the mean absolute error, the accuracy and the confusion_matrix
        print (metrics.mean_absolute_error(y_test,pred_test))
        print (metrics.accuracy_score(y_test,pred_test))
        print (confusion_matrix(y_test, clr.predict(X_test)))
        #summing mean absolute error and accuracy
        mae_sum+=metrics.mean_absolute_error(y_test,pred_test)
        acc_sum+=metrics.accuracy_score(y_test,pred_test)

    #calculating the mean across the 10 runs
    MAE=mae_sum/folds
    print ('MAE: ',  MAE)
    ACC=acc_sum/folds
    print ('ACC: ', ACC)
    csv_out1 = io.StringIO()

    #storing the accuracy statistics for the ML algorithm
    data = u"localtime,MAE,ACC";
    csv_out1.write(data)
    csv_out1.write(u'\n')
    row = [localtime,MAE,ACC];
    print(row)
    row_joined = (str(row).strip('[').strip(']'))
    csv_out1.write(row_joined)
    csv_out1.write(u'\n')
    s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_resource.Object("hashtagswahlpost", 'botdetectSTATS.csv').put(Body=csv_out1.getvalue())


#loading in the dataset
X = new_datareduced

X.head()
#defining the dependent variable as the botornot column in the dataset
y=X['botornot']

#removing user_id and bot_or_not column from the dataset with the independet variables
del X['botornot']
del X['user_id']
print(X.head())

#defining rf as the RandomForestClassifier
rf=RandomForestClassifier()

#Now starting the RandomForestClassifier in a ten fold set-up
kfold(rf,np.array(X),y)
